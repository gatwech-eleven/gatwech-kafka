# Next.js Documentation App for @gatwech/kafka

## Overview

A modern, searchable documentation site built with Next.js and Nextra for the `@gatwech/kafka` package.

## Tech Stack

### Core
- **Next.js 14** - App Router
- **Nextra** - Documentation framework
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling

### Features
- **MDX** - Interactive code examples
- **Algolia DocSearch** - Full-text search
- **Shiki** - Syntax highlighting
- **next-themes** - Dark mode

## Project Structure

```
docs/
├── app/
│   ├── layout.tsx
│   ├── page.tsx
│   └── [...slug]/page.tsx
├── content/
│   ├── getting-started/
│   │   ├── installation.mdx
│   │   ├── quick-start.mdx
│   │   └── configuration.mdx
│   ├── guides/
│   │   ├── producers.mdx
│   │   ├── consumers.mdx
│   │   ├── resilience.mdx
│   │   └── testing.mdx
│   ├── api/
│   │   ├── client.mdx
│   │   ├── topics.mdx
│   │   └── extensions.mdx
│   ├── examples/
│   │   ├── express.mdx
│   │   ├── nestjs.mdx
│   │   └── serverless.mdx
│   └── advanced/
│       ├── schema-registry.mdx
│       ├── monitoring.mdx
│       └── performance.mdx
├── components/
│   ├── CodeBlock.tsx
│   ├── ApiReference.tsx
│   └── InteractiveExample.tsx
├── public/
│   └── examples/
└── package.json
```

## Setup

### 1. Initialize Project

```bash
cd packages/kafka
pnpm create next-app@latest docs --typescript --tailwind --app
cd docs
pnpm add nextra nextra-theme-docs
```

### 2. Configure Next.js

```typescript
// next.config.mjs
import nextra from 'nextra';

const withNextra = nextra({
  theme: 'nextra-theme-docs',
  themeConfig: './theme.config.tsx',
  defaultShowCopyCode: true,
  latex: true,
});

export default withNextra({
  output: 'export',
  images: { unoptimized: true },
  basePath: process.env.BASE_PATH || '',
});
```

### 3. Theme Configuration

```tsx
// theme.config.tsx
import { DocsThemeConfig } from 'nextra-theme-docs';

const config: DocsThemeConfig = {
  logo: <span>@gatwech/kafka</span>,
  project: {
    link: 'https://github.com/yourusername/kafka',
  },
  docsRepositoryBase: 'https://github.com/yourusername/kafka/tree/main/docs',
  footer: {
    text: '@gatwech/kafka - Production-ready Kafka client for Node.js',
  },
  useNextSeoProps() {
    return {
      titleTemplate: '%s – @gatwech/kafka',
    };
  },
  search: {
    placeholder: 'Search documentation...',
  },
  sidebar: {
    defaultMenuCollapseLevel: 1,
    toggleButton: true,
  },
  toc: {
    backToTop: true,
  },
};

export default config;
```

## Content Examples

### Interactive Code Block

```mdx
// content/getting-started/quick-start.mdx
import { Tabs, Tab } from 'nextra-theme-docs';
import { CodeBlock } from '@/components/CodeBlock';

# Quick Start

<Tabs items={['Producer', 'Consumer']}>
  <Tab>
    <CodeBlock language="typescript" live>
      {`import { createKafkaClient } from '@gatwech/kafka';

const client = createKafkaClient({
  config: {
    clientId: 'my-service',
    brokers: ['localhost:9092'],
  },
});

await client.connect();
await client.sendMessage('user.registered', {
  userId: '123',
  email: 'user@example.com',
});`}
    </CodeBlock>
  </Tab>
  
  <Tab>
    <CodeBlock language="typescript" live>
      {`await client.consume(
  'user-group',
  'user.registered',
  async (payload) => {
    console.log('User registered:', payload);
  }
);`}
    </CodeBlock>
  </Tab>
</Tabs>
```

### API Reference Component

```tsx
// components/ApiReference.tsx
export function ApiReference({ 
  name, 
  signature, 
  params, 
  returns, 
  example 
}: ApiReferenceProps) {
  return (
    <div className="api-reference">
      <h3>{name}</h3>
      <CodeBlock language="typescript">{signature}</CodeBlock>
      
      <h4>Parameters</h4>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {params.map(p => (
            <tr key={p.name}>
              <td><code>{p.name}</code></td>
              <td><code>{p.type}</code></td>
              <td>{p.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
      
      <h4>Returns</h4>
      <p><code>{returns}</code></p>
      
      <h4>Example</h4>
      <CodeBlock language="typescript">{example}</CodeBlock>
    </div>
  );
}
```

### Interactive Playground

```tsx
// components/InteractiveExample.tsx
'use client';

import { useState } from 'react';
import { CodeBlock } from './CodeBlock';

export function InteractiveExample() {
  const [topic, setTopic] = useState('user.registered');
  const [message, setMessage] = useState('{ "userId": "123" }');
  
  const code = `
await client.sendMessage('${topic}', ${message});
  `.trim();
  
  return (
    <div className="interactive-example">
      <div className="controls">
        <input
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          placeholder="Topic name"
        />
        <textarea
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          placeholder="Message payload"
        />
      </div>
      
      <CodeBlock language="typescript" live>
        {code}
      </CodeBlock>
      
      <button onClick={() => {/* Run code */}}>
        Try it out
      </button>
    </div>
  );
}
```

## Key Features

### 1. Full-Text Search

```bash
# Add Algolia DocSearch
pnpm add @docsearch/react
```

```tsx
// theme.config.tsx
search: {
  component: <AlgoliaSearch />,
},
```

### 2. Version Switcher

```tsx
// components/VersionSwitcher.tsx
export function VersionSwitcher() {
  const versions = ['v0.0.9', 'v0.0.8', 'v0.0.7'];
  
  return (
    <select onChange={(e) => {
      window.location.href = `/${e.target.value}/docs`;
    }}>
      {versions.map(v => (
        <option key={v} value={v}>{v}</option>
      ))}
    </select>
  );
}
```

### 3. Code Playground

```tsx
// components/CodePlayground.tsx
import { LiveProvider, LiveEditor, LiveError, LivePreview } from 'react-live';

export function CodePlayground({ code }: { code: string }) {
  return (
    <LiveProvider code={code}>
      <LiveEditor />
      <LiveError />
      <LivePreview />
    </LiveProvider>
  );
}
```

### 4. Auto-Generated API Docs

```bash
# Generate from TypeScript
pnpm add typedoc typedoc-plugin-markdown
```

```json
// typedoc.json
{
  "entryPoints": ["../src/index.ts"],
  "out": "content/api",
  "plugin": ["typedoc-plugin-markdown"],
  "readme": "none"
}
```

## Content Structure

### Getting Started
- Installation
- Quick Start (< 5 min)
- Configuration
- First Message

### Guides
- Producers
  - Service Factory Pattern
  - Event Publishers
  - Batching
- Consumers
  - Type-Safe Consumers
  - Retry & DLQ
  - Consumer Groups
- Resilience
  - Circuit Breaker
  - Auto-Reconnection
  - Health Checks
- Testing
  - Unit Tests
  - Integration Tests
  - Mocking

### API Reference
- Client
  - createKafkaClient
  - connect/disconnect
  - sendMessage/sendBatch
- Topics
  - defineTopics
  - provisionTopics
- Extensions
  - Custom Extensions
  - Built-in Extensions

### Examples
- Express Integration
- NestJS Integration
- Serverless (AWS Lambda)
- Microservices Architecture

### Advanced
- Schema Registry
- Monitoring & Observability
- Performance Tuning
- Multi-Tenancy

## Deployment

### Vercel (Recommended)

```bash
# Install Vercel CLI
pnpm add -g vercel

# Deploy
cd docs
vercel --prod
```

### GitHub Pages

```yaml
# .github/workflows/docs.yml
name: Deploy Docs

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: pnpm/action-setup@v2
      - uses: actions/setup-node@v3
        with:
          node-version: 20
          cache: 'pnpm'
      
      - run: pnpm install
      - run: pnpm build
        working-directory: docs
      
      - uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./docs/out
```

### Cloudflare Pages

```bash
# Build command
pnpm build

# Output directory
out
```

## Analytics & Monitoring

### Vercel Analytics

```tsx
// app/layout.tsx
import { Analytics } from '@vercel/analytics/react';

export default function RootLayout({ children }) {
  return (
    <html>
      <body>
        {children}
        <Analytics />
      </body>
    </html>
  );
}
```

### Google Analytics

```tsx
// app/layout.tsx
import Script from 'next/script';

export default function RootLayout({ children }) {
  return (
    <html>
      <body>
        {children}
        <Script
          src={`https://www.googletagmanager.com/gtag/js?id=${GA_ID}`}
          strategy="afterInteractive"
        />
      </body>
    </html>
  );
}
```

## SEO Optimization

```tsx
// app/layout.tsx
export const metadata = {
  title: '@gatwech/kafka - Production Kafka Client',
  description: 'Lightweight, production-grade Kafka client for Node.js with strong typing and built-in reliability',
  keywords: ['kafka', 'nodejs', 'typescript', 'microservices'],
  openGraph: {
    title: '@gatwech/kafka',
    description: 'Production-ready Kafka client for Node.js',
    images: ['/og-image.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: '@gatwech/kafka',
    description: 'Production-ready Kafka client for Node.js',
    images: ['/og-image.png'],
  },
};
```

## Implementation Checklist

- [ ] Initialize Next.js project with Nextra
- [ ] Configure theme and navigation
- [ ] Migrate existing docs to MDX
- [ ] Create interactive components
- [ ] Add code playground
- [ ] Set up Algolia search
- [ ] Generate API docs from TypeScript
- [ ] Add version switcher
- [ ] Configure deployment (Vercel)
- [ ] Set up analytics
- [ ] Add SEO metadata
- [ ] Test on mobile/tablet
- [ ] Add feedback widget
- [ ] Configure custom domain

## Estimated Timeline

- **Week 1:** Setup + Migration
- **Week 2:** Interactive Components
- **Week 3:** API Docs + Search
- **Week 4:** Polish + Deploy

## Benefits

✅ **Modern UX** - Fast, responsive, beautiful
✅ **Interactive** - Live code examples
✅ **Searchable** - Full-text search with Algolia
✅ **Versioned** - Multiple version support
✅ **SEO-friendly** - Static generation
✅ **Mobile-first** - Responsive design
✅ **Dark mode** - Built-in theme switching

## Example Sites

- [Next.js Docs](https://nextjs.org/docs)
- [Tailwind CSS Docs](https://tailwindcss.com/docs)
- [SWR Docs](https://swr.vercel.app)
- [Nextra Demo](https://nextra.site)
