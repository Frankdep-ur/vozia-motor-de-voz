/**
 * VozIA — Motor de Voz  (versão Lovable Cloud)
 * ---------------------------------------------------------------------------
 * FASE 7: o motor novo passa a ler AGENTE e CONTATO do Supabase e a gravar
 *         a ligação nos Relatórios. A persona agora é editada no painel.
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
  .replace(/^https?:\/\//, "").replace(/\/$/, "");
const VOICE_BACKEND_SECRET = process.env.VOICE_BACKEND_SECRET || "";
const CLAUDE_MODEL = process.env.CLAUDE_MODEL || "claude-haiku-4-5-20251001";
const ELEVENLABS_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || "";
const TWILIO_FROM = process.env.TWILIO_FROM || "";
const MAX_CONCURRENT_CALLS = parseInt(process.env.MAX_CONCURRENT_CALLS || "3", 10);
const TRANSCRIPTION_PROVIDER = process.env.TRANSCRIPTION_PROVIDER || "Google";

// Qual motor o discador usa: "streams" (novo, voz clonada) ou "relay" (antigo)
const MOTOR_PADRAO = (process.env.MOTOR_PADRAO || "streams").toLowerCase();

// --- Botões de ajuste ---
const BARGE_MIN_CHARS = parseInt(process.env.BARGE_MIN_CHARS || "14", 10);
const MIN_FALA_PRIMEIRA = parseInt(process.env.MIN_FALA_PRIMEIRA || "18", 10);
const MIN_FALA_RESTO = parseInt(process.env.MIN_FALA_RESTO || "45", 10);
const MAX_FALA = parseInt(process.env.MAX_FALA || "110", 10);
const DG_ENDPOINTING = parseInt(process.env.DG_ENDPOINTING || "300", 10);
const SILENCIO_MS = parseInt(process.env.SILENCIO_MS || "9000", 10);
const FLUSH_MS = parseInt(process.env.FLUSH_MS || "900", 10);

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_USER_EMAIL = process.env.SUPABASE_USER_EMAIL || "";
const SUPABASE_USER_PASSWORD = process.env.SUPABASE_USER_PASSWORD || "";

const ELEVENLABS_VOICE_ID_CLONE = process.env.ELEVENLABS_VOICE_ID_CLONE || "";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_MODEL = process.env.ELEVENLABS_MODEL || "eleven_flash_v2_5";

const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }) : null;
const twilioClient =
  process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
    ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN) : null;

// ---- Supabase com cache: evita relogar a cada ligação (economiza ~500ms) ----
let _sbCache = null, _sbQuando = 0;
const SB_VALIDADE = 25 * 60 * 1000;

async function getSupabaseLogado(forcar = false) {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_USER_EMAIL || !SUPABASE_USER_PASSWORD) return null;
  if (!forcar && _sbCache && Date.now() - _sbQuando < SB_VALIDADE) return _sbCache;
  try {
    const client = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
      realtime: { transport: WebSocket },
    });
    const { data, error } = await client.auth.signInWithPassword({
      email: SUPABASE_USER_EMAIL, password: SUPABASE_USER_PASSWORD,
    });
    if (error) { console.error("[supabase] falha ao logar:", error.message); return null; }
    _sbCache = { client, userId: data.user?.id };
    _sbQuando = Date.now();
    console.log("[supabase] logado ✅");
    return _sbCache;
  } catch (e) {
    console.error("[supabase] exceção:", e?.message || e);
    return null;
  }
}

function avisarFaltando() {
  const faltando = [];
  if (!anthropic) faltando.push("ANTHROPIC_API_KEY");
  if (!twilioClient) faltando.push("TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN");
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) faltando.push("SUPABASE_URL/SUPABASE_ANON_KEY");
  if (!SUPABASE_USER_EMAIL || !SUPABASE_USER_PASSWORD) faltando.push("SUPABASE_USER_EMAIL/SUPABASE_USER_PASSWORD");
  if (!PUBLIC_HOST) faltando.push("PUBLIC_HOST");
  if (!TWILIO_FROM) faltando.push("TWILIO_FROM");
  if (!DEEPGRAM_API_KEY) faltando.push("DEEPGRAM_API_KEY");
  if (!ELEVENLABS_API_KEY) faltando.push("ELEVENLABS_API_KEY");
  if (faltando.length) console.warn("[VozIA] Variáveis não configuradas:", faltando.join(", "));
  console.log(
    `[VozIA] motor padrão: ${MOTOR_PADRAO} | barge-in≥${BARGE_MIN_CHARS} | ` +
    `fala ${MIN_FALA_PRIMEIRA}/${MIN_FALA_RESTO}/${MAX_FALA} | flush ${FLUSH_MS}ms`
  );
}

// ----------------------- Utilidades -----------------------
function escapeXml(s = "") {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}
function preencherNome(texto = "", nome = "") {
  if (!nome) return String(texto).replace(/\[(nome|NOME|nome do contato|NOME DO CONTATO)\]/g, "").replace(/\s{2,}/g, " ");
  return String(texto).replace(/\[(nome|NOME|nome do contato|NOME DO CONTATO)\]/g, nome);
}
// Primeiro nome, com inicial maiúscula
function primeiroNome(nome = "") {
  const p = String(nome).trim().split(/\s+/)[0] || "";
  return p ? p.charAt(0).toUpperCase() + p.slice(1).toLowerCase() : "";
}
function resolverVoz(agente) {
  const v = agente && agente.voz_id ? String(agente.voz_id).trim() : "";
  if (/^[A-Za-z0-9]{18,}$/.test(v)) return v;
  return ELEVENLABS_VOICE_ID_CLONE || ELEVENLABS_VOICE_ID;
}
function resolverVelocidade(agente) {
  const v = parseFloat(agente?.velocidade_fala);
  if (!isFinite(v)) return 1.0;
  return Math.min(1.2, Math.max(0.7, v));
}
function sanitizar(mensagens) {
  const out = [];
  for (const m of mensagens) {
    if (!m.content || !String(m.content).trim()) continue;
    if (out.length && out[out.length - 1].role === m.role) {
      out[out.length - 1].content += " " + m.content;
    } else out.push({ role: m.role, content: m.content });
  }
  while (out.length && out[0].role !== "user") out.shift();
  return out;
}
function quebrarSeLonga(texto, limite = MAX_FALA) {
  if (texto.length <= limite) return [texto];
  const partes = [];
  let atual = "";
  for (const pedaco of texto.split(/,\s*/)) {
    const cand = atual ? atual + ", " + pedaco : pedaco;
    if (cand.length > limite && atual) {
      partes.push(atual.endsWith(",") ? atual : atual + ","); atual = pedaco;
    } else atual = cand;
  }
  if (atual) partes.push(atual);
  const final = [];
  for (const p of partes) {
    if (p.length <= limite * 1.4) { final.push(p); continue; }
    let linha = "";
    for (const palavra of p.split(/\s+/)) {
      if ((linha + " " + palavra).trim().length > limite && linha) {
        final.push(linha.trim()); linha = palavra;
      } else linha = (linha + " " + palavra).trim();
    }
    if (linha) final.push(linha);
  }
  return final;
}

async function carregarAgente(client, agenteId) {
  if (!agenteId) return null;
  const { data, error } = await client.from("agentes").select("*").eq("id", agenteId).single();
  if (error) console.error("[banco] erro ao ler agente:", error.message);
  return data || null;
}
async function carregarContato(client, contatoId) {
  if (!contatoId) return null;
  const { data, error } = await client.from("contatos").select("*").eq("id", contatoId).single();
  if (error) console.error("[banco] erro ao ler contato:", error.message);
  return data || null;
}
async function carregarCampanha(client, campanhaId) {
  if (!campanhaId) return null;
  const { data } = await client.from("campanhas").select("*").eq("id", campanhaId).single();
  return data || null;
}

// ── Envelopa a persona do painel com as regras técnicas da ligação ──
function montarPersonaStreams(agente, contato, saudacao) {
  const base = (agente?.persona_prompt || "").trim() ||
    "Você é um atendente educado e prestativo de uma empresa.";
  const nome = primeiroNome(contato?.nome || "");
  const blocoNome = nome
    ? `O nome da pessoa com quem você está falando é ${nome}. Este nome veio do cadastro e é confiável — pode usá-lo naturalmente na conversa.`
    : `Você NÃO sabe o nome desta pessoa. Não use nome nenhum e não pergunte o nome mais de uma vez.`;

  return `${base}

════════ REGRAS TÉCNICAS DESTA LIGAÇÃO (não negociáveis) ════════

VOCÊ JÁ FALOU ISTO ASSIM QUE A PESSOA ATENDEU:
"${saudacao}"
Portanto NUNCA se apresente de novo, nem diga "oi", "alô", "aqui é o" ou o nome da
empresa outra vez. A conversa JÁ COMEÇOU. Continue de onde a pessoa respondeu.
Se ela só disser "sim", "pode" ou "oi", vá direto ao assunto.

SOBRE O NOME:
${blocoNome}
NUNCA chame a pessoa por um nome que você "ouviu" durante a ligação: a transcrição
do telefone erra nomes com frequência e chamar pelo nome errado queima a ligação.

TAMANHO DA RESPOSTA:
No MÁXIMO duas frases curtas por vez. Cada frase com no máximo quinze palavras.
Uma ideia por vez, nunca emende dois assuntos. Sempre devolva com uma pergunta curta.

EXPLIQUE ANTES DE PERGUNTAR:
Nunca peça um dado sem dar o motivo na mesma frase.
Se a pessoa disser que não entendeu, NÃO repita a pergunta com outras palavras:
explique o benefício em uma frase com exemplo concreto, e só depois pergunte de novo.

SE TE INTERROMPEREM:
Pare o assunto anterior e responda o que foi perguntado.

FORMATO DA FALA (isto vira áudio, não texto):
Números e valores por extenso: "duzentos e cinquenta reais", nunca "R$ 250".
Nada de listas, asteriscos, emojis ou qualquer formatação.
Quando o objetivo da ligação for cumprido, despeça-se de forma curta e simpática.`;
}

// ----------------------- App HTTP -----------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/", (req, res) => res.send("VozIA — motor de voz online ✅"));

// ---- TwiML do motor ANTIGO (Conversation Relay) ----
app.all("/twiml", (req, res) => {
  const campanhaId = req.query.campanha_id || "";
  const contatoId = req.query.contato_id || "";
  const saudacao = req.query.saudacao || "Olá, tudo bem? Você tem um minutinho?";
  const voice = req.query.voice || ELEVENLABS_VOICE_ID;
  const language = req.query.language || "pt-BR";
  const host = PUBLIC_HOST || req.headers.host;
  const wsUrl = `wss://${host}/ws`;
  res.type("text/xml").send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="${escapeXml(wsUrl)}" ttsProvider="ElevenLabs" voice="${escapeXml(
      voice)}" transcriptionProvider="${escapeXml(TRANSCRIPTION_PROVIDER)}" language="${escapeXml(
      language)}" welcomeGreeting="${escapeXml(saudacao)}" interruptible="speech">
      <Parameter name="campanha_id" value="${escapeXml(campanhaId)}"/>
      <Parameter name="contato_id" value="${escapeXml(contatoId)}"/>
    </ConversationRelay>
  </Connect>
</Response>`);
});

// ---- TwiML do motor NOVO: agora carrega os identificadores ----
app.all("/twiml-streams", (req, res) => {
  const host = PUBLIC_HOST || req.headers.host;
  const wsUrl = `wss://${host}/ws-streams`;
  const campanhaId = req.query.campanha_id || "";
  const contatoId = req.query.contato_id || "";
  const agenteId = req.query.agente_id || "";
  res.type("text/xml").send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${escapeXml(wsUrl)}">
      <Parameter name="campanha_id" value="${escapeXml(campanhaId)}"/>
      <Parameter name="contato_id" value="${escapeXml(contatoId)}"/>
      <Parameter name="agente_id" value="${escapeXml(agenteId)}"/>
    </Stream>
  </Connect>
</Response>`);
});

// ---- Discador ----
app.post("/campanhas/iniciar", async (req, res) => {
  const auth = req.headers.authorization || "";
  if (!VOICE_BACKEND_SECRET || auth !== `Bearer ${VOICE_BACKEND_SECRET}`) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!twilioClient) return res.status(500).json({ error: "Twilio não configurado" });
  const sb = await getSupabaseLogado();
  if (!sb) return res.status(500).json({ error: "login Supabase falhou" });
  const supabase = sb.client, userId = sb.userId;

  const campanhaId = req.body?.campanha_id;
  if (!campanhaId) return res.status(400).json({ error: "campanha_id é obrigatório" });

  try {
    const campanha = await carregarCampanha(supabase, campanhaId);
    if (!campanha) return res.status(404).json({ error: "campanha não encontrada" });
    const agente = await carregarAgente(supabase, campanha.agente_id);

    const { data: itens } = await supabase.from("campanha_contatos")
      .select("id, contato_id, status, tentativas")
      .eq("campanha_id", campanhaId).eq("status", "na_fila").limit(MAX_CONCURRENT_CALLS);
    if (!itens || itens.length === 0) {
      return res.json({ started: true, dialed: 0, message: "Nenhum contato na fila." });
    }

    const host = PUBLIC_HOST;
    if (!host) return res.status(500).json({ error: "PUBLIC_HOST não configurado" });
    const enc = encodeURIComponent;
    let dialed = 0;

    for (const item of itens) {
      const contato = await carregarContato(supabase, item.contato_id);
      if (!contato?.telefone) continue;

      let twimlUrl;
      if (MOTOR_PADRAO === "streams") {
        twimlUrl =
          `https://${host}/twiml-streams?campanha_id=${enc(campanhaId)}` +
          `&contato_id=${enc(item.contato_id)}&agente_id=${enc(campanha.agente_id || "")}`;
      } else {
        const saudacao = preencherNome(
          agente?.saudacao_inicial || "Olá, tudo bem? Você tem um minutinho?", contato?.nome);
        twimlUrl =
          `https://${host}/twiml?campanha_id=${enc(campanhaId)}&contato_id=${enc(item.contato_id)}` +
          `&saudacao=${enc(saudacao)}&voice=${enc(ELEVENLABS_VOICE_ID)}` +
          `&language=${enc(agente?.idioma || "pt-BR")}`;
      }

      try {
        const call = await twilioClient.calls.create({
          to: contato.telefone, from: TWILIO_FROM, url: twimlUrl, machineDetection: "Enable",
        });
        await supabase.from("ligacoes").insert({
          user_id: userId, campanha_id: campanhaId, contato_id: item.contato_id,
          status: "ligando", twilio_call_sid: call.sid, iniciada_em: new Date().toISOString(),
        });
        await supabase.from("campanha_contatos").update({
          status: "ligando", tentativas: (item.tentativas || 0) + 1,
          atualizado_em: new Date().toISOString(),
        }).eq("id", item.id);
        dialed++;
        console.log(`[discador] ligando (${MOTOR_PADRAO}) para ${contato.nome || contato.telefone}`);
      } catch (err) {
        console.error("[discador] erro ao ligar para", contato.telefone, err?.message);
        await supabase.from("campanha_contatos").update({ status: "falhou" }).eq("id", item.id);
      }
    }

    await supabase.from("campanhas").update({ status: "em_andamento" }).eq("id", campanhaId);
    res.json({ started: true, dialed, motor: MOTOR_PADRAO });
  } catch (e) {
    console.error("[/campanhas/iniciar] erro:", e);
    res.status(500).json({ error: "erro ao iniciar campanha" });
  }
});

// ---- Teste: agora aceita &agente=<id> ----
app.get("/streams/teste", async (req, res) => {
  const senha = req.query.senha || "";
  if (!VOICE_BACKEND_SECRET || senha !== VOICE_BACKEND_SECRET) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!twilioClient) return res.status(500).json({ error: "Twilio não configurado" });
  const digitos = String(req.query.para || "").replace(/\D/g, "");
  if (digitos.length < 12) return res.status(400).json({ error: "use ?para=5518999999999" });
  const para = "+" + digitos;
  const agenteId = req.query.agente || "";
  const contatoId = req.query.contato || "";
  const host = PUBLIC_HOST || req.headers.host;
  const enc = encodeURIComponent;

  try {
    const call = await twilioClient.calls.create({
      to: para, from: TWILIO_FROM,
      url: `https://${host}/twiml-streams?agente_id=${enc(agenteId)}&contato_id=${enc(contatoId)}`,
    });
    console.log("[streams/teste] ligando para", para, "agente:", agenteId || "(nenhum)");
    res.json({ ok: true, ligando_para: para, agente_id: agenteId || null, callSid: call.sid });
  } catch (e) {
    console.error("[streams/teste] erro:", e?.message);
    res.status(500).json({ error: e?.message || "erro ao ligar" });
  }
});

app.use((err, req, res, next) => {
  console.error("[erro nao tratado]", err);
  if (res.headersSent) return next(err);
  res.status(500).json({ error: "erro interno: " + (err?.message || "desconhecido") });
});

// ----------------------- WebSocket -----------------------
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const wssStreams = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const rota = (req.url || "").split("?")[0];
  if (rota === "/ws") wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  else if (rota === "/ws-streams") wssStreams.handleUpgrade(req, socket, head, (ws) => wssStreams.emit("connection", ws, req));
  else socket.destroy();
});

// ===================== MOTOR ANTIGO (Conversation Relay) =====================
const sessions = new Map();

wss.on("connection", (ws) => {
  const session = {
    callSid: null, campanhaId: null, contatoId: null, history: [], transcript: [],
    startedAt: Date.now(), currentStream: null, agente: null, contato: null,
    saudacao: "", systemPrompt: "", supabase: null, userId: null,
  };
  sessions.set(ws, session);

  ws.on("message", async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "setup") {
      session.callSid = msg.callSid;
      const p = msg.customParameters || {};
      session.campanhaId = p.campanha_id || "";
      session.contatoId = p.contato_id || "";
      let saudacao = "Olá!";
      const sb = await getSupabaseLogado();
      if (sb) {
        session.supabase = sb.client; session.userId = sb.userId;
        try {
          const campanha = await carregarCampanha(sb.client, session.campanhaId);
          session.agente = await carregarAgente(sb.client, campanha?.agente_id);
          session.contato = await carregarContato(sb.client, session.contatoId);
          if (session.agente?.saudacao_inicial)
            saudacao = preencherNome(session.agente.saudacao_inicial, session.contato?.nome);
        } catch (e) { console.error("[setup] erro:", e?.message); }
      }
      session.saudacao = saudacao;
      session.systemPrompt = montarPersonaStreams(session.agente, session.contato, saudacao);
      session.transcript.push(`Assistente: ${saudacao}`);
      console.log("[setup] callSid:", session.callSid);
      return;
    }

    if (msg.type === "prompt") {
      const fala = msg.voicePrompt || "";
      if (!fala.trim()) return;
      if (session.currentStream) { try { session.currentStream.abort(); } catch {} session.currentStream = null; }
      session.transcript.push(`Cliente: ${fala}`);
      session.history.push({ role: "user", content: fala });
      if (!anthropic) {
        ws.send(JSON.stringify({ type: "text", token: "Desculpe, tive um problema técnico.", last: true }));
        return;
      }
      try {
        let texto = "";
        const stream = anthropic.messages.stream({
          model: CLAUDE_MODEL, max_tokens: 120,
          system: session.systemPrompt, messages: sanitizar(session.history),
        });
        session.currentStream = stream;
        stream.on("text", (d) => {
          texto += d;
          try { ws.send(JSON.stringify({ type: "text", token: d, last: false })); } catch {}
        });
        await stream.finalMessage();
        ws.send(JSON.stringify({ type: "text", token: "", last: true }));
        session.currentStream = null;
        if (texto.trim()) {
          session.history.push({ role: "assistant", content: texto });
          session.transcript.push(`Assistente: ${texto}`);
        }
      } catch (e) {
        if (e?.name !== "APIUserAbortError" && e?.name !== "AbortError")
          console.error("[prompt] erro Claude:", e?.message);
        session.currentStream = null;
      }
      return;
    }

    if (msg.type === "interrupt") {
      if (session.currentStream) { try { session.currentStream.abort(); } catch {} session.currentStream = null; }
      return;
    }
    if (msg.type === "error") { console.error("[CR error]", msg.description || msg); return; }
  });

  ws.on("close", async () => {
    sessions.delete(ws);
    await gravarLigacao({
      supabase: session.supabase, callSid: session.callSid,
      campanhaId: session.campanhaId, contatoId: session.contatoId,
      transcricao: session.transcript.join("\n"),
      houveConversa: session.history.length > 0,
      duracao: Math.round((Date.now() - session.startedAt) / 1000),
    });
  });
});

// ---- Gravação nos Relatórios (usada pelos DOIS motores) ----
async function gravarLigacao({ supabase, callSid, campanhaId, contatoId, transcricao, houveConversa, duracao }) {
  if (!supabase || !callSid) {
    console.log("[relatorio] ligação de teste — nada a gravar no banco");
    return;
  }
  let resultado = null, sentimento = null, nota = null;
  if (anthropic && houveConversa && transcricao.trim()) {
    try {
      const r = await anthropic.messages.create({
        model: CLAUDE_MODEL, max_tokens: 250,
        messages: [{ role: "user", content:
          `Abaixo está a transcrição de uma ligação de prospecção. Responda APENAS com um JSON válido, sem texto extra e sem markdown, no formato exato: {"resultado":"resumo curto do que aconteceu e qual o próximo passo","sentimento":"positivo|neutro|negativo","nota": número de 1 a 10 avaliando o quanto este contato é promissor, ou null}. Transcrição:\n\n${transcricao}` }],
      });
      let txt = (r.content?.[0]?.text || "").trim().replace(/^```(json)?/i, "").replace(/```$/, "").trim();
      const obj = JSON.parse(txt);
      resultado = obj.resultado ?? null;
      sentimento = ["positivo", "neutro", "negativo"].includes(obj.sentimento) ? obj.sentimento : null;
      nota = typeof obj.nota === "number" ? obj.nota : null;
      console.log(`[relatorio] resumo: ${sentimento} | nota ${nota} | ${resultado}`);
    } catch (e) { console.error("[relatorio] erro ao resumir:", e?.message); }
  }
  try {
    await supabase.from("ligacoes").update({
      status: houveConversa ? "atendida" : "sem_resposta",
      duracao_segundos: duracao, transcricao, resultado, sentimento, nota,
      finalizada_em: new Date().toISOString(),
    }).eq("twilio_call_sid", callSid);
    if (campanhaId && contatoId) {
      await supabase.from("campanha_contatos").update({
        status: houveConversa ? "concluida" : "sem_resposta",
        atualizado_em: new Date().toISOString(),
      }).eq("campanha_id", campanhaId).eq("contato_id", contatoId);
    }
    console.log("[relatorio] ✅ ligação gravada:", callSid, `(${duracao}s)`);
  } catch (e) { console.error("[relatorio] erro ao gravar:", e?.message); }
}

// ===================== MOTOR NOVO (Media Streams) =====================
const pausa = (ms) => new Promise((r) => setTimeout(r, ms));

async function falarComMinhaVoz(ws, st, texto, meuTurno) {
  const vozId = st.vozId || ELEVENLABS_VOICE_ID_CLONE;
  if (!ELEVENLABS_API_KEY || !vozId || !st.streamSid) {
    console.error("[boca] faltando chave, voz ou streamSid");
    return false;
  }
  const inicio = Date.now();
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${vozId}/stream?output_format=ulaw_8000`;

  const pedir = async (comVelocidade) => {
    const vs = { stability: 0.5, similarity_boost: 0.8 };
    if (comVelocidade && st.velocidade && st.velocidade !== 1.0) vs.speed = st.velocidade;
    return fetch(url, {
      method: "POST",
      headers: { "xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({
        text: texto, model_id: ELEVENLABS_MODEL, language_code: "pt", voice_settings: vs,
      }),
    });
  };

  try {
    let resp = await pedir(true);
    if (!resp.ok && (resp.status === 400 || resp.status === 422) && st.velocidade !== 1.0) {
      console.warn("[boca] velocidade recusada — repetindo sem ela");
      st.velocidade = 1.0;
      resp = await pedir(false);
    }
    if (!resp.ok) {
      const erro = await resp.text();
      console.error("[boca] ElevenLabs recusou:", resp.status, erro.slice(0, 300));
      return false;
    }
    const audio = Buffer.from(await resp.arrayBuffer());
    const seg = (audio.length / 8000).toFixed(1);
    if (seg > 6) console.warn(`[boca] ⚠️ trecho longo: ${seg}s`);
    console.log(`[boca] ${seg}s de áudio em ${Date.now() - inicio}ms`);
    if (st.turno !== meuTurno) { console.log("[boca] interrompido antes de começar"); return false; }

    st.marcasPendentes++;
    const PEDACO = 640, POR_LEVA = 16, ESPERA = 800;
    let desde = 0;
    for (let i = 0; i < audio.length; i += PEDACO) {
      if (st.turno !== meuTurno || ws.readyState !== 1) {
        console.log("[boca] 🛑 parei no meio da fala");
        st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
        return false;
      }
      ws.send(JSON.stringify({
        event: "media", streamSid: st.streamSid,
        media: { payload: audio.subarray(i, i + PEDACO).toString("base64") },
      }));
      if (++desde >= POR_LEVA) { desde = 0; await pausa(ESPERA); }
    }
    if (st.turno !== meuTurno) { st.marcasPendentes = Math.max(0, st.marcasPendentes - 1); return false; }
    ws.send(JSON.stringify({ event: "mark", streamSid: st.streamSid, mark: { name: `t${meuTurno}` } }));
    return true;
  } catch (e) {
    console.error("[boca] erro:", e?.message || e);
    st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
    return false;
  }
}

function calarABoca(ws, st, motivo) {
  st.turno++; st.marcasPendentes = 0; st.falando = false;
  if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} }
  if (st.streamSid && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ event: "clear", streamSid: st.streamSid })); } catch {}
  }
  console.log(`[barge-in] ✋ CALEI A BOCA — ${motivo}`);
}

async function falarAvulso(ws, st, texto) {
  st.turno++;
  const meu = st.turno;
  st.falando = true;
  st.podeInterromperApos = Date.now() + 800;
  st.historico.push({ role: "assistant", content: texto });
  st.transcricao.push(`Agente: ${texto}`);
  console.log(`[resgate] "${texto}"`);
  await falarComMinhaVoz(ws, st, texto, meu);
}

async function pensarEResponder(ws, st, falaDoCliente) {
  if (!falaDoCliente || !anthropic) return;
  st.turno++;
  const meuTurno = st.turno;
  st.falando = true; st.claudePensando = true; st.marcasPendentes = 0;
  st.podeInterromperApos = Date.now() + 800;
  st.tentativasResgate = 0;

  const inicio = Date.now();
  let primeiraEm = 0, completo = "", buffer = "", pendente = "";
  let acabouTexto = false;
  const fila = [];

  const empurrar = (t) => {
    pendente = pendente ? pendente + " " + t : t;
    const minimo = fila.length === 0 ? MIN_FALA_PRIMEIRA : MIN_FALA_RESTO;
    if (pendente.length >= minimo) {
      for (const p of quebrarSeLonga(pendente)) fila.push(p);
      pendente = "";
    }
  };

  try {
    st.historico.push({ role: "user", content: falaDoCliente });
    st.transcricao.push(`Cliente: ${falaDoCliente}`);

    const stream = anthropic.messages.stream({
      model: CLAUDE_MODEL, max_tokens: 100,
      system: st.persona, messages: sanitizar(st.historico),
    });
    st.streamClaude = stream;

    stream.on("text", (d) => {
      buffer += d; completo += d;
      let m;
      while ((m = buffer.match(/^([\s\S]*?[.!?…]+)(\s|$)/))) {
        empurrar(m[1].trim());
        buffer = buffer.slice(m[0].length);
      }
    });

    const consumidor = (async () => {
      let i = 0;
      while (st.turno === meuTurno) {
        if (i < fila.length) {
          const trecho = fila[i++];
          if (!primeiraEm) {
            primeiraEm = Date.now() - inicio;
            console.log(`[cerebro] ⚡ primeira fala em ${primeiraEm}ms: "${trecho}"`);
          } else console.log(`[cerebro] continua: "${trecho}"`);
          await falarComMinhaVoz(ws, st, trecho, meuTurno);
        } else if (acabouTexto) return;
        else await pausa(50);
      }
      console.log("[cerebro] turno abortado");
    })();

    await stream.finalMessage();
    const resto = (pendente + " " + buffer).trim();
    if (resto) for (const p of quebrarSeLonga(resto)) fila.push(p);
    pendente = ""; buffer = "";
    acabouTexto = true; st.claudePensando = false;
    await consumidor;

    if (st.turno === meuTurno && completo.trim()) {
      st.historico.push({ role: "assistant", content: completo.trim() });
      st.transcricao.push(`Agente: ${completo.trim()}`);
      console.log(`[cerebro] turno completo em ${Date.now() - inicio}ms`);
    }
  } catch (e) {
    if (e?.name !== "APIUserAbortError" && e?.name !== "AbortError")
      console.error("[cerebro] erro:", e?.message || e);
  } finally {
    st.claudePensando = false;
    st.streamClaude = null;
  }
}

wssStreams.on("connection", (ws) => {
  const st = {
    streamSid: null, callSid: null, pacotes: 0, dg: null, dgPronto: false, fila: [],
    balde: "", ultimoInterim: "", flushTimer: null,
    historico: [], transcricao: [], turno: 0, falando: false, claudePensando: false,
    marcasPendentes: 0, podeInterromperApos: 0, streamClaude: null,
    calado_desde: Date.now(), tentativasResgate: 0, encerrando: false,
    // Fase 7
    campanhaId: "", contatoId: "", agenteId: "",
    agente: null, contato: null, supabase: null,
    persona: "", saudacao: "", vozId: "", velocidade: 1.0,
    iniciadoEm: Date.now(),
  };
  console.log("[streams] túnel aberto, aguardando áudio…");

  function limparFlush() { if (st.flushTimer) { clearTimeout(st.flushTimer); st.flushTimer = null; } }

  function despachar(fala, motivo) {
    limparFlush();
    st.balde = ""; st.ultimoInterim = "";
    const texto = (fala || "").trim();
    if (!texto) return;
    if (st.falando || st.claudePensando || st.marcasPendentes > 0) {
      console.log(`[ouvido] (ignorado, ainda falando) "${texto}"`);
      return;
    }
    console.log(`[ouvido] >>> pessoa disse: "${texto}"  [${motivo}]`);
    pensarEResponder(ws, st, texto);
  }

  function agendarFlush(ms) {
    limparFlush();
    st.flushTimer = setTimeout(() => {
      st.flushTimer = null;
      const fala = (st.balde || st.ultimoInterim || "").trim();
      if (fala) despachar(fala, "fechado por tempo");
    }, ms);
  }

  function abrirOuvido() {
    if (!DEEPGRAM_API_KEY) { console.error("[deepgram] chave não configurada!"); return; }
    const idioma = st.agente?.idioma || "pt-BR";
    const url =
      "wss://api.deepgram.com/v1/listen?encoding=mulaw&sample_rate=8000&channels=1" +
      `&language=${encodeURIComponent(idioma)}&model=nova-2` +
      "&punctuate=true&smart_format=true" +
      `&interim_results=true&endpointing=${DG_ENDPOINTING}&utterance_end_ms=1000&vad_events=true`;
    const dg = new WebSocket(url, { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } });
    st.dg = dg;

    dg.on("open", () => {
      st.dgPronto = true;
      console.log("[deepgram] ouvido conectado ✅");
      for (const b of st.fila) { try { dg.send(b); } catch {} }
      st.fila = [];
    });

    dg.on("message", (raw) => {
      let ev;
      try { ev = JSON.parse(raw.toString()); } catch { return; }
      if (ev.type === "UtteranceEnd") {
        const fala = (st.balde || st.ultimoInterim || "").trim();
        if (fala) despachar(fala, "UtteranceEnd");
        return;
      }
      if (ev.type !== "Results") return;
      const texto = (ev.channel?.alternatives?.[0]?.transcript || "").trim();
      if (!texto) return;

      st.calado_desde = Date.now();
      st.tentativasResgate = 0;
      const estaFalando = st.falando || st.marcasPendentes > 0 || st.claudePensando;

      if (estaFalando && texto.length >= BARGE_MIN_CHARS) {
        if (Date.now() > st.podeInterromperApos) {
          calarABoca(ws, st, `pessoa disse: "${texto}"`);
          limparFlush(); st.balde = ""; st.ultimoInterim = "";
        } else console.log(`[barge-in] (carência) "${texto}"`);
      }

      if (!ev.is_final) {
        console.log(`[ouvido] ouvindo… "${texto}"`);
        st.ultimoInterim = texto;
        agendarFlush(FLUSH_MS * 2);
        return;
      }

      console.log(`[ouvido] FINAL: "${texto}"${ev.speech_final ? "  <-- terminou" : ""}`);
      st.balde = (st.balde ? st.balde + " " : "") + texto;
      st.ultimoInterim = "";
      if (ev.speech_final) despachar(st.balde, "speech_final");
      else agendarFlush(FLUSH_MS);
    });

    dg.on("error", (e) => console.error("[deepgram] erro:", e?.message));
    dg.on("close", (c) => { st.dgPronto = false; console.log("[deepgram] desconectado (código", c + ")"); });
  }

  // Carrega agente/contato e começa a ligação
  async function iniciarConversa() {
    const t0 = Date.now();
    const sb = await getSupabaseLogado();
    if (sb) {
      st.supabase = sb.client;
      try {
        if (!st.agenteId && st.campanhaId) {
          const camp = await carregarCampanha(sb.client, st.campanhaId);
          st.agenteId = camp?.agente_id || "";
        }
        st.agente = await carregarAgente(sb.client, st.agenteId);
        st.contato = await carregarContato(sb.client, st.contatoId);
      } catch (e) { console.error("[banco] erro ao carregar contexto:", e?.message); }
    }

    if (st.agente) {
      console.log(`[agente] "${st.agente.nome}" carregado do painel em ${Date.now() - t0}ms`);
    } else {
      console.warn("[agente] ⚠️ nenhum agente carregado — usando padrão genérico");
    }
    if (st.contato?.nome) console.log(`[contato] falando com: ${st.contato.nome}`);

    st.saudacao = preencherNome(
      st.agente?.saudacao_inicial || "Olá, tudo bem? Você tem um minutinho?",
      st.contato?.nome
    ).trim();
    st.persona = montarPersonaStreams(st.agente, st.contato, st.saudacao);
    st.vozId = resolverVoz(st.agente);
    st.velocidade = resolverVelocidade(st.agente);
    console.log(`[agente] voz ${st.vozId} | velocidade ${st.velocidade}x`);

    const segEstimados = (st.saudacao.length / 16).toFixed(1);
    if (st.saudacao.length > 160) {
      console.warn(`[agente] ⚠️ saudação longa (${st.saudacao.length} chars, ~${segEstimados}s) — considere encurtar no painel`);
    }

    abrirOuvido();
    st.turno++;
    st.falando = true;
    st.podeInterromperApos = Date.now() + 1500;
    st.calado_desde = Date.now();
    st.transcricao.push(`Agente: ${st.saudacao}`);
    await falarComMinhaVoz(ws, st, st.saudacao, st.turno);
  }

  const vigia = setInterval(async () => {
    if (st.encerrando || !st.persona) return;
    const ocupado = st.falando || st.claudePensando || st.marcasPendentes > 0;
    if (ocupado || st.balde || st.ultimoInterim) { st.calado_desde = Date.now(); return; }
    const mudoHa = Date.now() - st.calado_desde;
    if (mudoHa < SILENCIO_MS) return;

    const RESGATES = ["Alô, você ainda está aí?", "Se preferir, eu ligo em outro momento. Pode ser?"];
    if (st.tentativasResgate < RESGATES.length) {
      const frase = RESGATES[st.tentativasResgate++];
      console.log(`[vigia] silêncio de ${Math.round(mudoHa / 1000)}s — resgate ${st.tentativasResgate}`);
      st.calado_desde = Date.now();
      await falarAvulso(ws, st, frase);
    } else {
      st.encerrando = true;
      console.log("[vigia] sem resposta — encerrando");
      await falarAvulso(ws, st, "Parece que a ligação caiu. Vou desligar, mas fico à disposição. Um abraço!");
      await pausa(5000);
      if (twilioClient && st.callSid) {
        try { await twilioClient.calls(st.callSid).update({ status: "completed" }); }
        catch (e) { console.error("[vigia] erro ao desligar:", e?.message); }
      }
    }
  }, 2000);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.event === "start") {
      st.streamSid = msg.start?.streamSid || null;
      st.callSid = msg.start?.callSid || null;
      const p = msg.start?.customParameters || {};
      st.campanhaId = p.campanha_id || "";
      st.contatoId = p.contato_id || "";
      st.agenteId = p.agente_id || "";
      console.log(`[streams] início — callSid: ${st.callSid} | agente: ${st.agenteId || "(nenhum)"}`);
      iniciarConversa();
      return;
    }

    if (msg.event === "media") {
      st.pacotes++;
      if (st.pacotes % 1500 === 0) console.log(`[streams] áudio fluindo… ${st.pacotes} pacotes`);
      const audio = Buffer.from(msg.media.payload, "base64");
      if (st.dgPronto) { try { st.dg.send(audio); } catch {} }
      else { st.fila.push(audio); if (st.fila.length > 500) st.fila.shift(); }
      return;
    }

    if (msg.event === "mark") {
      st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
      if (st.marcasPendentes === 0 && !st.claudePensando) {
        st.falando = false;
        st.calado_desde = Date.now();
        console.log("[streams] terminou de falar — escutando 👂");
      }
      return;
    }

    if (msg.event === "stop") {
      console.log(`[streams] fim do túnel — pacotes: ${st.pacotes}`);
      return;
    }
  });

  ws.on("close", async () => {
    clearInterval(vigia); limparFlush();
    const duracao = Math.round((Date.now() - st.iniciadoEm) / 1000);
    console.log(`[streams] túnel fechado (${duracao}s)`);
    console.log("═══════ TRANSCRIÇÃO DA LIGAÇÃO ═══════");
    for (const l of st.transcricao) console.log("  " + l);
    console.log("══════════════════════════════════════");
    if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} }
    if (st.dg) { try { st.dg.close(); } catch {} }

    await gravarLigacao({
      supabase: st.campanhaId ? st.supabase : null,
      callSid: st.callSid, campanhaId: st.campanhaId, contatoId: st.contatoId,
      transcricao: st.transcricao.join("\n"),
      houveConversa: st.historico.some((m) => m.role === "user"),
      duracao,
    });
  });

  ws.on("error", (e) => { clearInterval(vigia); limparFlush(); console.error("[streams] erro:", e?.message); });
});

server.listen(PORT, () => {
  console.log(`[VozIA] motor de voz ouvindo na porta ${PORT}`);
  avisarFaltando();
});
