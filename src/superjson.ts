import SuperJSON from 'superjson'

export const superjson = new SuperJSON()

export function extendTypes(handler: (superjson: SuperJSON) => void) {
  handler(superjson)
}

export function clone<T>(obj: T): T {
  return superjson.deserialize(superjson.serialize(obj))
}
