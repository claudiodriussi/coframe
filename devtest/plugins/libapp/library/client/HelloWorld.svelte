<script lang="ts">
	/**
	 * Library Plugin - Hello World Component
	 *
	 * Example plugin component from backend
	 * Located in: coframe/devtest/plugins/libapp/library/client/
	 */

	interface Props {
		message?: string;
		userName?: string;
	}

	let { message = 'Hello from Library Plugin!', userName = 'Guest' }: Props = $props();
</script>

<div class="library-hello-world p-6 border-2 border-purple-300 rounded-lg bg-purple-50">
	<div class="flex items-center gap-3 mb-4">
		<svg class="w-8 h-8 text-purple-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
			<path
				stroke-linecap="round"
				stroke-linejoin="round"
				stroke-width="2"
				d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"
			/>
		</svg>
		<h3 class="text-xl font-bold text-purple-800">Library Plugin Component</h3>
	</div>

	<div class="space-y-3">
		<p class="text-purple-900 text-lg font-semibold">{message}</p>
		<p class="text-purple-700">
			Welcome, <strong>{userName}</strong>!
		</p>

		<div class="mt-4 p-4 bg-white rounded border border-purple-200">
			<p class="text-sm text-gray-600">
				<strong>Plugin Info:</strong>
			</p>
			<ul class="text-xs text-gray-500 mt-2 space-y-1">
				<li>• Component: HelloWorld.svelte</li>
				<li>• Plugin: library</li>
				<li>• Location: coframe/devtest/plugins/libapp/library/client/</li>
				<li>• Registry ID: library.hello</li>
			</ul>
		</div>

		<div class="mt-4">
			<button
				onclick={() => alert('Hello from plugin component!')}
				class="px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700 transition-colors"
			>
				Test Plugin Action
			</button>
		</div>
	</div>
</div>
