import type { APIRoute } from 'astro';
import { buildIndex } from '../llms';

export const GET: APIRoute = async ({ site }) =>
  new Response(await buildIndex(site!), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
