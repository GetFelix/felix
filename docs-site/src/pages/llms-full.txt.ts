import type { APIRoute } from 'astro';
import { buildFull } from '../llms';

export const GET: APIRoute = async ({ site }) =>
  new Response(await buildFull(site!), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
