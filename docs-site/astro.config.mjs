import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import mermaid from 'astro-mermaid';

export default defineConfig({
  site: 'https://gabloe.github.io',
  base: '/felix',
  integrations: [
    mermaid({ autoTheme: true, enableLog: false }),
    starlight({
      // Fonts are self-hosted via @fontsource so the site has no runtime
      // dependency on Google Fonts, and so first paint is not gated on a
      // third-party connection. Inter for UI/prose, JetBrains Mono for the
      // 1710 code fences. Order matters: these must land before custom.css,
      // which is what actually points `--sl-font`/`--sl-font-mono` at them.
      //
      // Layout is tuned for code- and table-heavy technical docs; see the
      // custom.css header for why Starlight's defaults do not fit this content.
      customCss: [
        '@fontsource-variable/inter',
        '@fontsource-variable/jetbrains-mono',
        // Only the one weight/style the site title uses, so this costs a single
        // small file rather than the whole family.
        '@fontsource/lobster-two/700-italic.css',
        './src/styles/custom.css',
      ],
      title: 'Felix',
      description: 'Low-latency QUIC-based pub/sub and distributed cache system',
      // The header renders the logo at ~32px. The full logo — ringed badge, whole
      // cat, and the "Felix" script inside it — carries far too much detail to
      // survive that, and the script is redundant anyway because the site title
      // renders as text right beside it. `logo-mark.png` is a derived mark: the
      // cat's head alone, in the same circular badge, so it stays recognisably
      // the Felix logo while actually reading at header size. The full logo is
      // still used for the landing-page hero, where it has room.
      logo: {
        src: './src/assets/logo-mark.png',
        alt: 'Felix',
      },
      favicon: '/logo.png',
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/gabloe/felix',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/gabloe/felix/edit/main/docs-site/',
      },
      lastUpdated: true,
      sidebar: [
        { label: 'Home', slug: 'index' },
        {
          label: 'Getting Started',
          items: [
            { label: 'Why Felix?', slug: 'getting-started/why-felix' },
            { label: 'What Felix Is For', slug: 'getting-started/what-felix-is-for' },
            { label: 'Overview', slug: 'getting-started/overview' },
            { label: 'Quickstart', slug: 'getting-started/quickstart' },
            { label: 'Installation', slug: 'getting-started/installation' },
          ],
        },
        {
          label: 'Demos',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'demos/overview' },
            { label: 'Cross-broker Publishing', slug: 'demos/cross-broker-cluster' },
            { label: 'Slow-consumer Isolation', slug: 'demos/slow-consumer-isolation' },
            { label: 'Local State Divergence', slug: 'demos/state-divergence' },
            { label: 'Queue Semantics', slug: 'demos/queue-semantics' },
            { label: 'Leader vs Quorum', slug: 'demos/cluster-consistency' },
            { label: 'Notifications', slug: 'demos/notifications' },
            { label: 'Orders Pipeline', slug: 'demos/orders' },
          ],
        },
        {
          label: 'Architecture',
          collapsed: true,
          items: [
            { label: 'An Architecture Tour', slug: 'architecture/tour' },
            { label: 'System Design', slug: 'architecture/system-design' },
            { label: 'Components', slug: 'architecture/components' },
            { label: 'Wire Protocol', slug: 'architecture/wire-protocol' },
            { label: 'Durable Storage', slug: 'architecture/durable-storage' },
            { label: 'Durable Segment Format', slug: 'architecture/storage-format' },
            { label: 'Semantics', slug: 'architecture/semantics' },
            { label: 'Projections', slug: 'architecture/projections' },
            { label: 'How Felix Is Tested', slug: 'architecture/testing' },
            { label: 'Metadata Raft', slug: 'architecture/metadata-raft' },
          ],
        },
        {
          label: 'Clients',
          collapsed: true,
          items: [
            { label: 'Choosing a Client', slug: 'clients/overview' },
            { label: 'Rust', slug: 'clients/rust' },
            { label: 'Python', slug: 'clients/python' },
            { label: 'TypeScript', slug: 'clients/typescript' },
          ],
        },
        {
          label: 'API Documentation',
          collapsed: true,
          items: [
            { label: 'Broker API', slug: 'api/broker-api' },
            { label: 'Control Plane API', slug: 'api/control-plane-api' },
          ],
        },
        {
          label: 'Features',
          collapsed: true,
          items: [
            { label: 'QUIC Transport', slug: 'features/quic-transport' },
            { label: 'Pub/Sub Streaming', slug: 'features/pubsub' },
            { label: 'Queues and Consumer Groups', slug: 'features/queues' },
            { label: 'Atomic Commits', slug: 'features/atomic-commits' },
            { label: 'Distributed Cache', slug: 'features/cache' },
            { label: 'Kafka Compatibility', slug: 'features/kafka' },
            { label: 'Performance Tuning', slug: 'features/performance' },
            { label: 'Performance & Platform Notes', slug: 'features/performance-platform-notes' },
            { label: 'Benchmarks', slug: 'features/benchmarks' },
            { label: 'Real-Network Performance (Azure)', slug: 'features/real-network-performance' },
            { label: 'Case Study: Throughput Ceiling', slug: 'features/performance-case-study' },
            { label: 'Observability', slug: 'features/observability' },
            { label: 'Security', slug: 'features/security' },
          ],
        },
        {
          label: 'Deployment',
          collapsed: true,
          items: [
            { label: 'Local Development', slug: 'deployment/local' },
            { label: 'Docker Compose', slug: 'deployment/docker-compose' },
            { label: 'Kubernetes', slug: 'deployment/kubernetes' },
            { label: 'Adding and Removing Brokers', slug: 'deployment/scaling' },
            { label: 'Moving Shards by Hand', slug: 'deployment/moving-shards' },
            { label: 'Control-plane HA', slug: 'deployment/control-plane-ha' },
            { label: 'Backup and Restore', slug: 'deployment/backup-and-restore' },
            { label: 'Graceful Shutdown', slug: 'deployment/graceful-shutdown' },
            { label: 'Upgrades & Compatibility', slug: 'deployment/upgrades' },
          ],
        },
        {
          label: 'Reference',
          collapsed: true,
          items: [
            { label: 'Configuration', slug: 'reference/configuration' },
            { label: 'Environment Variables', slug: 'reference/environment-variables' },
            { label: 'Troubleshooting', slug: 'reference/troubleshooting' },
            { label: 'FAQ', slug: 'reference/faq' },
          ],
        },
        {
          label: 'Development',
          collapsed: true,
          items: [
            { label: 'Contributing', slug: 'development/contributing' },
            { label: 'Building & Testing', slug: 'development/building' },
            { label: 'Fuzzing', slug: 'development/fuzzing' },
            { label: 'Project Structure', slug: 'development/project-structure' },
            { label: 'How Felix Works', slug: 'development/how-felix-works' },
            { label: 'Internals: The Publish Path', slug: 'development/internals-publish' },
            { label: 'Internals: Subscribe & Fanout', slug: 'development/internals-subscribe' },
            { label: 'Internals: Backpressure & Core Sharding', slug: 'development/internals-concurrency' },
          ],
        },
      ],
    }),
  ],
});
