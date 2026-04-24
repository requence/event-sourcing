import { useEffect, useRef } from 'react'

// ── Shader sources ────────────────────────────────────────────────────────────

const VERT = /* glsl */ `
  attribute vec2 a_position;
  void main() {
    gl_Position = vec4(a_position, 0.0, 1.0);
  }
`

const FRAG = /* glsl */ `
  precision highp float;

  uniform vec2  u_resolution;
  uniform float u_time;
  uniform vec2  u_mouse;
  uniform float u_mouseMix;

  uniform float u_gridSpacing;
  uniform float u_dotRadius;
  uniform vec3  u_dotColor;
  uniform vec3  u_tintColor;
  uniform float u_morphRadius;
  uniform float u_morphStrength;
  uniform float u_fadeInner;
  uniform float u_fadeOuter;
  uniform float u_tintInner;
  uniform float u_tintOuter;

  #define MAX_RIPPLES 4
  uniform vec2  u_ripplePos[MAX_RIPPLES];
  uniform float u_rippleAge[MAX_RIPPLES];
  uniform float u_rippleSpeed;
  uniform float u_rippleWidth;
  uniform float u_rippleStrength;
  uniform float u_rippleDuration;

  void main() {
    vec2 pixel = gl_FragCoord.xy;
    vec2 mousePixel = u_mouse * u_resolution;
    vec2 cellIndex = floor(pixel / u_gridSpacing);

    float bestDist = 9999.0;
    float bestBlueMix = 0.0;

    for (float iy = -3.0; iy <= 3.0; iy += 1.0) {
      for (float ix = -3.0; ix <= 3.0; ix += 1.0) {
        vec2 dotOriginal = (cellIndex + vec2(ix, iy) + 0.5) * u_gridSpacing;

        vec2 delta = dotOriginal - mousePixel;
        float dist = length(delta);
        float mRadius = u_morphRadius;
        float mStrength = u_morphStrength * u_mouseMix;

        vec2 dotPos = dotOriginal;
        if (dist < mRadius && dist > 0.001) {
          float t = 1.0 - dist / mRadius;
          float falloff = t * t * (3.0 - 2.0 * t);
          vec2 dir = delta / dist;
          dotPos = dotOriginal + dir * falloff * mStrength;
        }

        for (int r = 0; r < MAX_RIPPLES; r++) {
          float age = u_rippleAge[r];
          if (age < 0.0) continue;

          vec2 ripplePixel = u_ripplePos[r] * u_resolution;
          float rippleRadius = age * u_rippleSpeed;
          vec2 rDelta = dotPos - ripplePixel;
          float rDist = length(rDelta);

          float distFromRing = abs(rDist - rippleRadius);

          if (distFromRing < u_rippleWidth && rDist > 0.001) {
            float ringFalloff = 1.0 - distFromRing / u_rippleWidth;
            ringFalloff = ringFalloff * ringFalloff;

            float lifeFade = 1.0 - age / u_rippleDuration;
            lifeFade = lifeFade * lifeFade;

            vec2 rDir = rDelta / rDist;
            dotPos += rDir * ringFalloff * lifeFade * u_rippleStrength;
          }
        }

        float d = length(pixel - dotPos);
        if (d < bestDist) {
          bestDist = d;
          bestBlueMix = (1.0 - smoothstep(u_tintInner, u_tintOuter, dist)) * u_mouseMix;

          for (int r = 0; r < MAX_RIPPLES; r++) {
            float age = u_rippleAge[r];
            if (age < 0.0) continue;
            vec2 ripplePixel = u_ripplePos[r] * u_resolution;
            float rippleRadius = age * u_rippleSpeed;
            float rDist = length(dotOriginal - ripplePixel);
            float distFromRing = abs(rDist - rippleRadius);
            if (distFromRing < u_rippleWidth) {
              float ringTint = 1.0 - distFromRing / u_rippleWidth;
              float lifeFade = 1.0 - age / u_rippleDuration;
              bestBlueMix = max(bestBlueMix, ringTint * lifeFade * lifeFade);
            }
          }
        }
      }
    }

    float dotMask = 1.0 - smoothstep(u_dotRadius - 0.5, u_dotRadius + 0.5, bestDist);
    float distToMouse = length(pixel - mousePixel);
    float proximityFade = smoothstep(u_fadeInner, u_fadeOuter, distToMouse);
    float fade = mix(1.0, proximityFade, u_mouseMix);
    vec3 tintedDotColor = mix(u_dotColor, u_tintColor, bestBlueMix * 0.65);
    float alpha = dotMask * fade;
    vec3 color = tintedDotColor * alpha;
    gl_FragColor = vec4(color, alpha);
  }
`

// ── WebGL helpers ─────────────────────────────────────────────────────────────

function compileShader(
  gl: WebGLRenderingContext,
  type: number,
  source: string,
): WebGLShader {
  const shader = gl.createShader(type)!
  gl.shaderSource(shader, source)
  gl.compileShader(shader)
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const log = gl.getShaderInfoLog(shader)
    gl.deleteShader(shader)
    throw new Error(`Shader compile error: ${log}`)
  }
  return shader
}

function createProgram(
  gl: WebGLRenderingContext,
  vert: string,
  frag: string,
): WebGLProgram {
  const vs = compileShader(gl, gl.VERTEX_SHADER, vert)
  const fs = compileShader(gl, gl.FRAGMENT_SHADER, frag)
  const program = gl.createProgram()!
  gl.attachShader(program, vs)
  gl.attachShader(program, fs)
  gl.linkProgram(program)
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const log = gl.getProgramInfoLog(program)
    gl.deleteProgram(program)
    throw new Error(`Program link error: ${log}`)
  }
  return program
}

function parseColor(css: string): [number, number, number] {
  const hexMatch = /^#([0-9a-f]{3,8})$/i.exec(css)
  if (hexMatch) {
    let hex = hexMatch[1]!
    if (hex.length === 3 || hex.length === 4) {
      hex = hex
        .split('')
        .map((c) => c + c)
        .join('')
    }
    const r = parseInt(hex.slice(0, 2), 16) / 255
    const g = parseInt(hex.slice(2, 4), 16) / 255
    const b = parseInt(hex.slice(4, 6), 16) / 255
    return [r, g, b]
  }
  return [1, 1, 1]
}

// ── Config ────────────────────────────────────────────────────────────────────

const GRID_SPACING = 20
const DOT_RADIUS = 0.5
const DOT_COLOR = '#777777'
const TINT_COLOR = '#ff6900'
const MORPH_RADIUS = 500
const MIN_MORPH_STRENGTH = 10
const MAX_MORPH_STRENGTH = 40
const FADE_INNER = 10
const FADE_OUTER = 150
const BUILDUP_SPEED = 0.01
const TINT_INNER = 90
const TINT_OUTER = 100
const POINTER_EASING = 0.1
const RIPPLE_SPEED = 600
const RIPPLE_WIDTH = 80
const RIPPLE_STRENGTH = 25
const RIPPLE_DURATION = 1.2

export default function SplashBackground() {
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const canvas = document.createElement('canvas')
    canvas.style.display = 'block'
    canvas.style.width = '100%'
    canvas.style.height = '100%'
    container.appendChild(canvas)

    const gl = canvas.getContext('webgl', {
      antialias: false,
      alpha: true,
      premultipliedAlpha: true,
    })
    if (!gl) return

    const program = createProgram(gl, VERT, FRAG)
    gl.useProgram(program)

    const posBuffer = gl.createBuffer()!
    gl.bindBuffer(gl.ARRAY_BUFFER, posBuffer)
    gl.bufferData(
      gl.ARRAY_BUFFER,
      new Float32Array([-1, -1, 1, -1, -1, 1, -1, 1, 1, -1, 1, 1]),
      gl.STATIC_DRAW,
    )

    const aPos = gl.getAttribLocation(program, 'a_position')
    gl.enableVertexAttribArray(aPos)
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0)

    const uResolution = gl.getUniformLocation(program, 'u_resolution')
    const uTime = gl.getUniformLocation(program, 'u_time')
    const uMouse = gl.getUniformLocation(program, 'u_mouse')
    const uMouseMix = gl.getUniformLocation(program, 'u_mouseMix')
    const uGridSpacing = gl.getUniformLocation(program, 'u_gridSpacing')
    const uDotRadius = gl.getUniformLocation(program, 'u_dotRadius')
    const uDotColor = gl.getUniformLocation(program, 'u_dotColor')
    const uTintColor = gl.getUniformLocation(program, 'u_tintColor')
    const uMorphRadius = gl.getUniformLocation(program, 'u_morphRadius')
    const uMorphStrength = gl.getUniformLocation(program, 'u_morphStrength')
    const uFadeInner = gl.getUniformLocation(program, 'u_fadeInner')
    const uFadeOuter = gl.getUniformLocation(program, 'u_fadeOuter')
    const uTintInner = gl.getUniformLocation(program, 'u_tintInner')
    const uTintOuter = gl.getUniformLocation(program, 'u_tintOuter')
    const uRippleSpeed = gl.getUniformLocation(program, 'u_rippleSpeed')
    const uRippleWidth = gl.getUniformLocation(program, 'u_rippleWidth')
    const uRippleStrength = gl.getUniformLocation(program, 'u_rippleStrength')
    const uRippleDuration = gl.getUniformLocation(program, 'u_rippleDuration')
    const uRipplePos: (WebGLUniformLocation | null)[] = []
    const uRippleAge: (WebGLUniformLocation | null)[] = []
    for (let i = 0; i < 4; i++) {
      uRipplePos.push(gl.getUniformLocation(program, `u_ripplePos[${i}]`))
      uRippleAge.push(gl.getUniformLocation(program, `u_rippleAge[${i}]`))
    }

    const dc = parseColor(DOT_COLOR)
    const tc = parseColor(TINT_COLOR)
    gl.uniform3f(uDotColor, dc[0], dc[1], dc[2])
    gl.uniform3f(uTintColor, tc[0], tc[1], tc[2])

    let w = 1
    let h = 1
    let mouseNorm = { x: 0.5, y: 0.5 }
    let smoothMouse = { x: 0.5, y: 0.5 }
    let pointerInside = false
    let currentMouseMix = 0
    let currentBuildUp = 0
    let smoothStrength = MIN_MORPH_STRENGTH
    let lastPointerMoveTime = 0

    const MAX_RIPPLES = 4
    const ripples: { x: number; y: number; startTime: number }[] = []

    const syncSize = (width: number, height: number) => {
      w = width
      h = height
      const dpr = window.devicePixelRatio || 1
      canvas.width = Math.round(w * dpr)
      canvas.height = Math.round(h * dpr)
      gl.viewport(0, 0, canvas.width, canvas.height)
      gl.uniform1f(uGridSpacing, GRID_SPACING * dpr)
      gl.uniform1f(uDotRadius, DOT_RADIUS * dpr)
      gl.uniform1f(uMorphRadius, MORPH_RADIUS * dpr)
      gl.uniform1f(uTintInner, TINT_INNER * dpr)
      gl.uniform1f(uTintOuter, TINT_OUTER * dpr)
      gl.uniform1f(uRippleSpeed, RIPPLE_SPEED * dpr)
      gl.uniform1f(uRippleWidth, RIPPLE_WIDTH * dpr)
      gl.uniform1f(uRippleStrength, RIPPLE_STRENGTH * dpr)
      gl.uniform1f(uRippleDuration, RIPPLE_DURATION)
    }

    const ro = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (!entry) return
      syncSize(entry.contentRect.width, entry.contentRect.height)
    })
    ro.observe(container)
    syncSize(container.clientWidth || 300, container.clientHeight || 300)

    // Listen on window since the canvas is behind all page content (z-index: 0)
    // and can't receive pointer events directly.
    const onPointerMove = (e: PointerEvent) => {
      mouseNorm = {
        x: e.clientX / window.innerWidth,
        y: 1.0 - e.clientY / window.innerHeight,
      }
      lastPointerMoveTime = performance.now()
      if (!pointerInside) {
        smoothMouse = { ...mouseNorm }
        pointerInside = true
        currentBuildUp = 0
        smoothStrength = MIN_MORPH_STRENGTH
      }
    }
    const onPointerLeave = () => {
      pointerInside = false
    }
    window.addEventListener('pointermove', onPointerMove)
    document.documentElement.addEventListener('pointerleave', onPointerLeave)

    const onClick = (e: PointerEvent) => {
      const clickNorm = {
        x: e.clientX / window.innerWidth,
        y: 1.0 - e.clientY / window.innerHeight,
      }
      ripples.push({
        x: clickNorm.x,
        y: clickNorm.y,
        startTime: performance.now(),
      })
      while (ripples.length > MAX_RIPPLES) {
        ripples.shift()
      }
    }
    window.addEventListener('pointerdown', onClick)

    let rafId = 0
    const startTime = performance.now()

    const animate = () => {
      rafId = requestAnimationFrame(animate)

      const now = performance.now()
      const elapsed = (now - startTime) * 0.001
      const dpr = window.devicePixelRatio || 1

      const ease = Math.min(Math.max(POINTER_EASING, 0), 1)
      smoothMouse.x += (mouseNorm.x - smoothMouse.x) * ease
      smoothMouse.y += (mouseNorm.y - smoothMouse.y) * ease

      const idleMs = now - lastPointerMoveTime
      const isResting = idleMs > 50 && pointerInside
      const buildUpTarget = isResting ? 1 : 0
      currentBuildUp += (buildUpTarget - currentBuildUp) * BUILDUP_SPEED

      const targetStrength =
        MIN_MORPH_STRENGTH +
        (MAX_MORPH_STRENGTH - MIN_MORPH_STRENGTH) * currentBuildUp
      smoothStrength += (targetStrength - smoothStrength) * 0.3

      const strengthRatio =
        MAX_MORPH_STRENGTH > 0 ? smoothStrength / MAX_MORPH_STRENGTH : 1
      const effectiveFadeInner = FADE_INNER * strengthRatio
      const effectiveFadeOuter = FADE_OUTER * strengthRatio

      const mixTarget = pointerInside ? 1 : 0
      currentMouseMix += (mixTarget - currentMouseMix) * 0.05

      gl.uniform2f(uResolution, canvas.width, canvas.height)
      gl.uniform1f(uTime, elapsed)
      gl.uniform2f(uMouse, smoothMouse.x, smoothMouse.y)
      gl.uniform1f(uMouseMix, currentMouseMix)
      gl.uniform1f(uMorphStrength, smoothStrength * dpr)
      gl.uniform1f(uFadeInner, effectiveFadeInner * dpr)
      gl.uniform1f(uFadeOuter, effectiveFadeOuter * dpr)

      for (let i = 0; i < MAX_RIPPLES; i++) {
        const r = ripples[i]
        if (r) {
          const age = (now - r.startTime) * 0.001
          if (age > RIPPLE_DURATION) {
            gl.uniform1f(uRippleAge[i], -1.0)
          } else {
            gl.uniform2f(uRipplePos[i], r.x, r.y)
            gl.uniform1f(uRippleAge[i], age)
          }
        } else {
          gl.uniform1f(uRippleAge[i], -1.0)
        }
      }
      while (
        ripples.length > 0 &&
        (now - ripples[0]!.startTime) * 0.001 > RIPPLE_DURATION
      ) {
        ripples.shift()
      }

      gl.drawArrays(gl.TRIANGLES, 0, 6)
    }
    animate()

    return () => {
      cancelAnimationFrame(rafId)
      ro.disconnect()
      window.removeEventListener('pointermove', onPointerMove)
      document.documentElement.removeEventListener('pointerleave', onPointerLeave)
      window.removeEventListener('pointerdown', onClick)
      gl.deleteBuffer(posBuffer)
      gl.deleteProgram(program)
      container.removeChild(canvas)
    }
  }, [])

  return (
    <div
      ref={containerRef}
      className="splash-bg"
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 0,
        background: '#18181b',
        overflow: 'hidden',
      }}
      aria-hidden="true"
    />
  )
}
