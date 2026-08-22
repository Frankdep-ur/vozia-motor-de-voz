/**
 * VozIA — Motor de Voz  (versão Lovable Cloud)
 * ---------------------------------------------------------------------------
 * FASE 6 (corrigida): barge-in real + fala em frases curtas.
 * ---------------------------------------------------------------------------
 */

import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import Anthropic from "@anthropic-ai/sdk";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";

// ----------------------- Configuração -----------------------
const PORT = process.env.PORT || 8080;
const PUBLIC_HOST = (process.env.PUBLIC_HOST || process.env.RAILWAY_PUBLIC_DOMAIN || "")
  .replace(/^https?:\/\//, "")
  .replace(/\/$/, "");
const VOICE_BACKEND_SECRET = process.env.VOICE_BACKEND_SECRET || "";
const CLAUDE_MODEL = process.env.CLAUDE_MODEL || "claude-haiku-4-5-20251001";
const ELEVENLABS_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || "";
const TWILIO_FROM = process.env.TWILIO_FROM || "";
const MAX_CONCURRENT_CALLS = parseInt(process.env.MAX_CONCURRENT_CALLS || "3", 10);
const TRANSCRIPTION_PROVIDER = process.env.TRANSCRIPTION_PROVIDER || "Google";

// Quantos caracteres a pessoa precisa falar pra cortar a fala do agente.
// Se ele cortar sozinho (eco da própria voz), AUMENTE este número.
const BARGE_MIN_CHARS = parseInt(process.env.BARGE_MIN_CHARS || "5", 10);

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_USER_EMAIL = process.env.SUPABASE_USER_EMAIL || "";
const SUPABASE_USER_PASSWORD = process.env.SUPABASE_USER_PASSWORD || "";

const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;
const twilioClient =
  process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
    ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)
    : null;

async function getSupabaseLogado() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_USER_EMAIL || !SUPABASE_USER_PASSWORD) {
    return null;
  }
  try {
    const client = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
      realtime: { transport: WebSocket },
    });
    const { data, error } = await client.auth.signInWithPassword({
      email: SUPABASE_USER_EMAIL,
      password: SUPABASE_USER_PASSWORD,
    });
    if (error) {
      console.error("[supabase] falha ao logar:", error.message);
      return null;
    }
    return { client, userId: data.user?.id };
  } catch (e) {
    console.error("[supabase] exceção ao conectar/logar:", e?.message || e);
    return null;
  }
}

function avisarFaltando() {
  const faltando = [];
  if (!anthropic) faltando.push("ANTHROPIC_API_KEY");
  if (!twilioClient) faltando.push("TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN");
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) faltando.push("SUPABASE_URL/SUPABASE_ANON_KEY");
  if (!SUPABASE_USER_EMAIL || !SUPABASE_USER_PASSWORD)
    faltando.push("SUPABASE_USER_EMAIL/SUPABASE_USER_PASSWORD");
  if (!PUBLIC_HOST) faltando.push("PUBLIC_HOST (ou RAILWAY_PUBLIC_DOMAIN)");
  if (!ELEVENLABS_VOICE_ID) faltando.push("ELEVENLABS_VOICE_ID");
  if (!TWILIO_FROM) faltando.push("TWILIO_FROM");
  if (!DEEPGRAM_API_KEY) faltando.push("DEEPGRAM_API_KEY (motor novo)");
  if (!ELEVENLABS_API_KEY) faltando.push("ELEVENLABS_API_KEY (motor novo)");
  if (!ELEVENLABS_VOICE_ID_CLONE) faltando.push("ELEVENLABS_VOICE_ID_CLONE (motor novo)");
  if (faltando.length)
    console.warn("[VozIA] Variáveis ainda não configuradas:", faltando.join(", "));

  if (ELEVENLABS_VOICE_ID && ELEVENLABS_VOICE_ID === ELEVENLABS_VOICE_ID_CLONE) {
    console.warn(
      "[VozIA] ATENÇÃO: ELEVENLABS_VOICE_ID está igual à voz clonada! " +
      "O motor antigo (Conversation Relay) vai cair em voz padrão em inglês."
    );
  }
  console.log(`[VozIA] barge-in: corta a partir de ${BARGE_MIN_CHARS} caracteres`);
}

// ----------------------- Utilidades -----------------------
function escapeXml(s = "") {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function preencherNome(texto = "", nome = "") {
  if (!nome) return texto;
  return texto.replace(/\[(nome|NOME|nome do contato|NOME DO CONTATO)\]/g, nome);
}

function resolverVoz(agente) {
  const v = agente && agente.voz_id ? String(agente.voz_id).trim() : "";
  if (/^[A-Za-z0-9]{18,}$/.test(v)) return v;
  return ELEVENLABS_VOICE_ID;
}

function sanitizar(mensagens) {
  const out = [];
  for (const m of mensagens) {
    if (!m.content || !String(m.content).trim()) continue;
    if (out.length && out[out.length - 1].role === m.role) {
      out[out.length - 1].content += " " + m.content;
    } else {
      out.push({ role: m.role, content: m.content });
    }
  }
  while (out.length && out[0].role !== "user") out.shift();
  return out;
}

// Quebra frase muito longa na vírgula, pra ninguém falar 6 segundos seguidos
function quebrarSeLonga(frase, limite = 130) {
  if (frase.length <= limite) return [frase];
  const
