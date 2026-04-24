// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import react from '@astrojs/react';
import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
	vite: {
		plugins: [tailwindcss()],
	},
	integrations: [
		react(),
		starlight({
			title: 'Event Sourcing',
			logo: {
				src: './public/requence-wordmark.svg',
				replacesTitle: false,
			},
			favicon: '/logo.svg',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/requence/event-sourcing' }],
			expressiveCode: {
				themes: ['dark-plus'],
				styleOverrides: {
					borderColor: 'var(--color-zinc-700)',
					borderRadius: '0.375rem',
					codeBackground: '#09090b',
				},
			},
			customCss: ['./src/styles/custom.css'],
			components: {
				PageFrame: './src/components/overrides/PageFrame.astro',
				ThemeSelect: './src/components/overrides/ThemeSelect.astro',
			},
			sidebar: [
				{
					label: 'Concepts',
					items: [
						{ label: 'Introduction', slug: 'concepts/01-introduction' },
						{ label: 'Aggregate Roots', slug: 'concepts/02-aggregate-roots' },
						{ label: 'Projections', slug: 'concepts/03-projections' },
						{ label: 'Process Managers', slug: 'concepts/04-process-managers' },
						{ label: 'Event Listeners', slug: 'concepts/05-event-handlers' },
						{ label: 'Storage Adapters', slug: 'concepts/06-storage-adapters' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'createEventStore', slug: 'reference/01-create-event-store' },
						{ label: 'createAggregateRoot', slug: 'reference/02-create-aggregate-root' },
						{ label: 'Projections', slug: 'reference/03-create-projection' },
						{ label: 'Process Managers', slug: 'reference/04-create-process-manager' },
						{ label: 'Event Listeners', slug: 'reference/05-create-event-listener' },
						{ label: 'WrappedEvent', slug: 'reference/06-wrapped-event' },
						{ label: 'Errors', slug: 'reference/07-errors' },
					],
				},
				{
					label: 'Guides',
					items: [
						{ label: 'User Management', slug: 'guides/01-user-management' },
					],
				},
			],
		}),
	],
});
