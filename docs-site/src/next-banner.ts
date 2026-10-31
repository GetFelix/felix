// Puts the "next release" notice on every page of the /next/ build.
// Registered only when DOCS_BASE is set (see astro.config.mjs).
import { defineRouteMiddleware } from '@astrojs/starlight/route-data';

export const onRequest = defineRouteMiddleware((context) => {
  context.locals.starlightRoute.entry.data.banner = {
    content:
      'You\'re reading the docs for the next release. For the current stable release, see the <a href="/" data-docs-root>main docs</a>.',
  };
});
