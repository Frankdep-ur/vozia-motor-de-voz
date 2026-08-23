/**
 * VozIA — Motor de Voz  (versão Lovable Cloud)
 * ---------------------------------------------------------------------------
 * FASE 6b: corrige saudação duplicada, prosódia picotada e devolve os logs.
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

// Barge-in: quantos caracteres a pessoa precisa falar pra cortar a fala.
// Cortou sozinho? AUMENTE. Não corta nunca? DIMINUA.
const BARGE_MIN_CHARS = parseInt(process.env.BARGE_MIN_CHARS || "14", 10);
// Prosódia: tamanho mínimo de texto antes de mandar pra voz.
const MIN_FALA_PRIMEIRA = parseInt(process.env.MIN_FALA_PRIMEIRA || "28", 10);
const MIN_FALA_RESTO = parseInt(process.env.MIN_FALA_RESTO || "100", 10);

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
    console.warn("[VozIA] ATENÇÃO: ELEVENLABS_VOICE_ID igual à voz clonada! O motor antigo vai falhar.");
  }
  console.log(
    `[VozIA] barge-in ≥${BARGE_MIN_CHARS} chars | prosódia ${MIN_FALA_PRIMEIRA}/${MIN_FALA_RESTO}`
  );
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

function quebrarSeLonga(frase, limite = 170) {
  if (frase.length <= limite) return [frase];
  const partes = [];
  let atual = "";
  for (const pedaco of frase.split(/,\s*/)) {
    const candidato = atual ? atual + ", " + pedaco : pedaco;
    if (candidato.length > limite && atual) {
      partes.push(atual.endsWith(",") ? atual : atual + ",");
      atual = pedaco;
    } else {
      atual = candidato;
    }
  }
  if (atual) partes.push(atual);
  return partes;
}

async function carregarContexto(client, campanhaId, contatoId) {
  const { data: campanha } = await client
    .from("campanhas")
    .select("*")
    .eq("id", campanhaId)
    .single();
  let agente = null,
    contato = null;
  if (campanha?.agente_id) {
    const r = await client.from("agentes").select("*").eq("id", campanha.agente_id).single();
    agente = r.data;
  }
  if (contatoId) {
    const r = await client.from("contatos").select("*").eq("id", contatoId).single();
    contato = r.data;
  }
  return { campanha, agente, contato };
}

function montarSystemPrompt(agente, contato, saudacao) {
  const persona =
    agente?.persona_prompt || "Você é um atendente educado e prestativo de uma empresa.";
  const idioma = agente?.idioma || "pt-BR";
  const nome = contato?.nome || "o cliente";
  const idiomaTexto =
    idioma === "pt-BR"
      ? "português do Brasil"
      : idioma === "pt-PT"
      ? "português de Portugal"
      : idioma;
  return `${persona}

CONTEXTO DESTA LIGAÇÃO:
- Você está falando ao telefone com: ${nome}.
- Você JÁ disse a saudação inicial: "${saudacao}". Não repita a saudação; apenas continue a conversa de forma natural a partir do que a pessoa responder.

COMO FALAR (isto é uma ligação de voz, não um chat):
- Fale em ${idiomaTexto}, com frases curtas e naturais, uma ideia de cada vez.
- Escreva números e valores por extenso (ex: "nove", "vinte reais") para a voz pronunciar corretamente.
- Não use listas, asteriscos, emojis nem qualquer formatação: apenas texto que será falado.
- Seja breve. Assim que o objetivo da ligação for cumprido, despeça-se de forma simpática e curta.`;
}

// ----------------------- App HTTP -----------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/", (req, res) => res.send("VozIA — motor de voz online ✅"));

app.all("/twiml", (req, res) => {
  const campanhaId = req.query.campanha_id || "";
  const contatoId = req.query.contato_id || "";
  const saudacao = req.query.saudacao || "Olá, tudo bem? Você tem um minutinho?";
  const voice = req.query.voice || ELEVENLABS_VOICE_ID;
  const language = req.query.language || "pt-BR";

  const host = PUBLIC_HOST || req.headers.host;
  const wsUrl = `wss://${host}/ws`;

  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="${escapeXml(wsUrl)}" ttsProvider="ElevenLabs" voice="${escapeXml(
      voice
    )}" transcriptionProvider="${escapeXml(TRANSCRIPTION_PROVIDER)}" language="${escapeXml(language)}" welcomeGreeting="${escapeXml(
      saudacao
    )}" interruptible="speech">
      <Parameter name="campanha_id" value="${escapeXml(campanhaId)}"/>
      <Parameter name="contato_id" value="${escapeXml(contatoId)}"/>
    </ConversationRelay>
  </Connect>
</Response>`;
  res.type("text/xml").send(twiml);
});

app.post("/campanhas/iniciar", async (req, res) => {
  const auth = req.headers.authorization || "";
  if (!VOICE_BACKEND_SECRET || auth !== `Bearer ${VOICE_BACKEND_SECRET}`) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!twilioClient) return res.status(500).json({ error: "Twilio não configurado" });
  const sb = await getSupabaseLogado();
  if (!sb) return res.status(500).json({ error: "login Supabase falhou" });
  const supabase = sb.client;
  const userId = sb.userId;

  const campanhaId = req.body?.campanha_id;
  if (!campanhaId) return res.status(400).json({ error: "campanha_id é obrigatório" });

  try {
    const { data: campanha } = await supabase
      .from("campanhas")
      .select("*")
      .eq("id", campanhaId)
      .single();
    if (!campanha) return res.status(404).json({ error: "campanha não encontrada" });

    let agente = null;
    if (campanha.agente_id) {
      const r = await supabase.from("agentes").select("*").eq("id", campanha.agente_id).single();
      agente = r.data;
    }

    const { data: itens } = await supabase
      .from("campanha_contatos")
      .select("id, contato_id, status, tentativas")
      .eq("campanha_id", campanhaId)
      .eq("status", "na_fila")
      .limit(MAX_CONCURRENT_CALLS);

    if (!itens || itens.length === 0) {
      return res.json({ started: true, dialed: 0, message: "Nenhum contato na fila." });
    }

    const host = PUBLIC_HOST;
    if (!host) return res.status(500).json({ error: "PUBLIC_HOST não configurado" });

    const enc = encodeURIComponent;
    let dialed = 0;
    for (const item of itens) {
      const { data: contato } = await supabase
        .from("contatos")
        .select("*")
        .eq("id", item.contato_id)
        .single();
      if (!contato?.telefone) continue;

      const saudacao = preencherNome(
        agente?.saudacao_inicial || "Olá, tudo bem? Você tem um minutinho?",
        contato?.nome
      );
      const voice = resolverVoz(agente);
      const language = agente?.idioma || "pt-BR";

      const twimlUrl =
        `https://${host}/twiml?campanha_id=${enc(campanhaId)}&contato_id=${enc(item.contato_id)}` +
        `&saudacao=${enc(saudacao)}&voice=${enc(voice)}&language=${enc(language)}`;

      try {
        const call = await twilioClient.calls.create({
          to: contato.telefone,
          from: TWILIO_FROM,
          url: twimlUrl,
          machineDetection: "Enable",
        });

        await supabase.from("ligacoes").insert({
          user_id: userId,
          campanha_id: campanhaId,
          contato_id: item.contato_id,
          status: "ligando",
          twilio_call_sid: call.sid,
          iniciada_em: new Date().toISOString(),
        });
        await supabase
          .from("campanha_contatos")
          .update({
            status: "ligando",
            tentativas: (item.tentativas || 0) + 1,
            atualizado_em: new Date().toISOString(),
          })
          .eq("id", item.id);
        dialed++;
      } catch (err) {
        console.error("[discador] erro ao ligar para", contato.telefone, err?.message);
        await supabase.from("campanha_contatos").update({ status: "falhou" }).eq("id", item.id);
      }
    }

    await supabase.from("campanhas").update({ status: "em_andamento" }).eq("id", campanhaId);
    res.json({ started: true, dialed });
  } catch (e) {
    console.error("[/campanhas/iniciar] erro:", e);
    res.status(500).json({ error: "erro ao iniciar campanha" });
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
  if (rota === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  } else if (rota === "/ws-streams") {
    wssStreams.handleUpgrade(req, socket, head, (ws) => wssStreams.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});

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
      if (sb && session.campanhaId) {
        session.supabase = sb.client;
        session.userId = sb.userId;
        try {
          const { agente, contato } = await carregarContexto(
            sb.client, session.campanhaId, session.contatoId
          );
          session.agente = agente;
          session.contato = contato;
          if (agente?.saudacao_inicial)
            saudacao = preencherNome(agente.saudacao_inicial, contato?.nome);
        } catch (e) {
          console.error("[setup] erro ao carregar contexto:", e?.message);
        }
      }
      session.saudacao = saudacao;
      session.systemPrompt = montarSystemPrompt(session.agente, session.contato, saudacao);
      session.transcript.push(`Assistente: ${saudacao}`);
      console.log("[setup] callSid:", session.callSid, "campanha:", session.campanhaId);
      return;
    }

    if (msg.type === "prompt") {
      const fala = msg.voicePrompt || "";
      if (!fala.trim()) return;

      if (session.currentStream) {
        try { session.currentStream.abort(); } catch {}
        session.currentStream = null;
      }

      session.transcript.push(`Cliente: ${fala}`);
      session.history.push({ role: "user", content: fala });

      if (!anthropic) {
        ws.send(JSON.stringify({
          type: "text", token: "Desculpe, estou com um problema técnico no momento.", last: true,
        }));
        return;
      }

      try {
        let texto = "";
        const stream = anthropic.messages.stream({
          model: CLAUDE_MODEL,
          max_tokens: 300,
          system: session.systemPrompt,
          messages: sanitizar(session.history),
        });
        session.currentStream = stream;
        stream.on("text", (delta) => {
          texto += delta;
          try { ws.send(JSON.stringify({ type: "text", token: delta, last: false })); } catch {}
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
      if (session.currentStream) {
        try { session.currentStream.abort(); } catch {}
        session.currentStream = null;
      }
      return;
    }

    if (msg.type === "error") {
      console.error("[CR error]", msg.description || msg);
      return;
    }
  });

  ws.on("close", async () => {
    sessions.delete(ws);
    await finalizarLigacao(session);
  });
});

async function finalizarLigacao(session) {
  if (!session.supabase || !session.callSid) return;
  const supabase = session.supabase;
  const duracao = Math.round((Date.now() - session.startedAt) / 1000);
  const transcricao = session.transcript.join("\n");
  const houveConversa = session.history.length > 0;

  let resultado = null, sentimento = null, nota = null;
  if (anthropic && houveConversa) {
    try {
      const r = await anthropic.messages.create({
        model: CLAUDE_MODEL,
        max_tokens: 200,
        messages: [{
          role: "user",
          content: `Abaixo está a transcrição de uma ligação. Responda APENAS com um JSON válido, sem texto extra e sem markdown, no formato exato: {"resultado":"resumo curto do que aconteceu","sentimento":"positivo|neutro|negativo","nota": número de 1 a 10 que o cliente deu, ou null}. Transcrição:\n\n${transcricao}`,
        }],
      });
      let txt = (r.content?.[0]?.text || "").trim()
        .replace(/^```(json)?/i, "").replace(/```$/, "").trim();
      const obj = JSON.parse(txt);
      resultado = obj.resultado ?? null;
      sentimento = ["positivo", "neutro", "negativo"].includes(obj.sentimento) ? obj.sentimento : null;
      nota = typeof obj.nota === "number" ? obj.nota : null;
    } catch (e) {
      console.error("[finalizar] erro ao extrair resumo:", e?.message);
    }
  }

  try {
    await supabase.from("ligacoes").update({
      status: houveConversa ? "atendida" : "sem_resposta",
      duracao_segundos: duracao, transcricao, resultado, sentimento, nota,
      finalizada_em: new Date().toISOString(),
    }).eq("twilio_call_sid", session.callSid);

    if (session.campanhaId && session.contatoId) {
      await supabase.from("campanha_contatos").update({
        status: houveConversa ? "concluida" : "sem_resposta",
        atualizado_em: new Date().toISOString(),
      }).eq("campanha_id", session.campanhaId).eq("contato_id", session.contatoId);
    }
    console.log("[finalizar] ligação salva:", session.callSid, "duração:", duracao + "s");
  } catch (e) {
    console.error("[finalizar] erro ao salvar:", e?.message);
  }
}

// ============================================================================
// ======  MOTOR NOVO (Media Streams) — FASES 2 a 6b  =========================
// ============================================================================

const ELEVENLABS_VOICE_ID_CLONE = process.env.ELEVENLABS_VOICE_ID_CLONE || "";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_MODEL = process.env.ELEVENLABS_MODEL || "eleven_flash_v2_5";

// Saudação mais curta (antes eram 7,9 segundos — longo demais pra abertura)
const SAUDACAO_STREAMS =
  "Alô, tudo bem? Aqui é o Rafael, da Invite Energy. " +
  "É rapidinho, sobre desconto na conta de luz. Posso falar?";

function montarPersonaStreams(saudacao) {
  return `Você é o Rafael, consultor da Invite Energy, empresa de energia solar por assinatura.

⚠️ VOCÊ JÁ FALOU ISTO AGORA HÁ POUCO, ASSIM QUE A PESSOA ATENDEU:
"${saudacao}"

Portanto:
- NUNCA se apresente de novo. A pessoa já sabe quem você é e de onde você é.
- NUNCA diga "oi", "alô", "tudo bem", "aqui é o Rafael" ou "da Invite Energy" outra vez.
- A conversa JÁ COMEÇOU. Continue de onde a pessoa respondeu, direto no assunto.
- Se a pessoa disser apenas "sim", "pode", "oi" ou "alô", ela está te liberando pra falar:
  vá direto pra primeira pergunta de qualificação, sem rodeio.

O QUE VOCÊ OFERECE:
- Desconto de até trinta por cento na conta de luz.
- Sem obra, sem placa, sem investimento nenhum.
- A pessoa continua com a mesma distribuidora, só recebe crédito de energia limpa.
- Regulamentado pela ANEEL.
- Só vale a pena pra quem tem conta acima de duzentos e cinquenta reais.

SEU OBJETIVO, NESTA ORDEM:
1. Confirmar se a pessoa é titular da conta de luz.
2. Descobrir quanto vem a conta por mês.
3. Se passar de duzentos e cinquenta reais, oferecer mandar detalhes pelo WhatsApp.
4. Você NÃO fecha contrato por telefone.

REGRA DE TAMANHO (A MAIS IMPORTANTE):
- No MÁXIMO duas frases por resposta, e frases curtas.
- Uma ideia por vez. Nunca emende dois assuntos.
- Sempre termine devolvendo a bola: uma pergunta curta.

REGRA SOBRE NOMES (CRÍTICA):
- A transcrição do telefone erra nomes o tempo todo.
- NUNCA chame a pessoa por um nome que você "ouviu" na ligação.
- Sem certeza absoluta, não use nome nenhum.

SE TE INTERROMPEREM:
- Largue o assunto anterior e responda o que foi perguntado.
- Não retome o que estava dizendo, a menos que peçam.

REGRAS ABSOLUTAS:
- Nunca peça CPF, senha, dados bancários ou cartão.
- Nunca prometa valor exato. Sempre "até trinta por cento".
- Sem interesse? Agradeça com simpatia e encerre. Não insista.

COMO FALAR:
- Português do Brasil, informal e caloroso, como gente de verdade.
- Números por extenso: "duzentos e cinquenta reais", nunca "R$ 250".
- Nada de listas, asteriscos ou emojis. Só o texto falado.`;
}

const PERSONA_STREAMS = montarPersonaStreams(SAUDACAO_STREAMS);

app.get("/streams/teste", async (req, res) => {
  const senha = req.query.senha || "";
  if (!VOICE_BACKEND_SECRET || senha !== VOICE_BACKEND_SECRET) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!twilioClient) return res.status(500).json({ error: "Twilio não configurado" });

  const digitos = String(req.query.para || "").replace(/\D/g, "");
  if (digitos.length < 12) {
    return res.status(400).json({ error: "use ?para=5518999999999" });
  }
  const para = "+" + digitos;
  const host = PUBLIC_HOST || req.headers.host;

  try {
    const call = await twilioClient.calls.create({
      to: para, from: TWILIO_FROM, url: `https://${host}/twiml-streams`,
    });
    console.log("[streams/teste] ligando para", para, "callSid:", call.sid);
    res.json({ ok: true, ligando_para: para, callSid: call.sid });
  } catch (e) {
    console.error("[streams/teste] erro:", e?.message);
    res.status(500).json({ error: e?.message || "erro ao ligar" });
  }
});

app.all("/twiml-streams", (req, res) => {
  const host = PUBLIC_HOST || req.headers.host;
  const wsUrl = `wss://${host}/ws-streams`;
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${escapeXml(wsUrl)}" />
  </Connect>
</Response>`;
  res.type("text/xml").send(twiml);
});

const pausa = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------- A BOCA ----------------
async function falarComMinhaVoz(ws, st, texto, meuTurno) {
  if (!ELEVENLABS_API_KEY || !ELEVENLABS_VOICE_ID_CLONE || !st.streamSid) {
    console.error("[boca] faltando chave, voz ou streamSid");
    return false;
  }

  const inicio = Date.now();
  const url =
    `https://api.elevenlabs.io/v1/text-to-speech/${ELEVENLABS_VOICE_ID_CLONE}/stream` +
    `?output_format=ulaw_8000`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({
        text: texto,
        model_id: ELEVENLABS_MODEL,
        language_code: "pt",
        voice_settings: { stability: 0.5, similarity_boost: 0.8 },
      }),
    });

    if (!resp.ok) {
      const erro = await resp.text();
      console.error("[boca] ElevenLabs recusou:", resp.status, erro.slice(0, 300));
      return false;
    }

    const audio = Buffer.from(await resp.arrayBuffer());
    console.log(`[boca] ${(audio.length / 8000).toFixed(1)}s de áudio em ${Date.now() - inicio}ms`);

    if (st.turno !== meuTurno) {
      console.log("[boca] interrompido antes de começar — descartado");
      return false;
    }

    st.marcasPendentes++;

    const PEDACO = 640;
    const POR_LEVA = 16;   // ~1,28s de áudio por leva
    const ESPERA = 800;    // envia 1,6x mais rápido que toca: colchão saudável
    let desde = 0;

    for (let i = 0; i < audio.length; i += PEDACO) {
      if (st.turno !== meuTurno || ws.readyState !== 1) {
        console.log("[boca] 🛑 parei no meio da fala");
        st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
        return false;
      }
      ws.send(JSON.stringify({
        event: "media",
        streamSid: st.streamSid,
        media: { payload: audio.subarray(i, i + PEDACO).toString("base64") },
      }));
      if (++desde >= POR_LEVA) { desde = 0; await pausa(ESPERA); }
    }

    if (st.turno !== meuTurno) {
      st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
      return false;
    }
    ws.send(JSON.stringify({
      event: "mark", streamSid: st.streamSid, mark: { name: `t${meuTurno}` },
    }));
    return true;
  } catch (e) {
    console.error("[boca] erro:", e?.message || e);
    st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
    return false;
  }
}

function calarABoca(ws, st, motivo) {
  st.turno++;
  st.marcasPendentes = 0;
  st.falando = false;
  if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} }
  if (st.streamSid && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ event: "clear", streamSid: st.streamSid })); } catch {}
  }
  console.log(`[barge-in] ✋ CALEI A BOCA — ${motivo}`);
}

// ---------------- O CÉREBRO ----------------
async function pensarEResponder(ws, st, falaDoCliente) {
  if (!falaDoCliente || !anthropic) return;

  st.turno++;
  const meuTurno = st.turno;
  st.falando = true;
  st.claudePensando = true;
  st.marcasPendentes = 0;
  st.podeInterromperApos = Date.now() + 800;

  const inicio = Date.now();
  let primeiraEm = 0, completo = "", buffer = "", pendente = "";
  let acabouTexto = false;
  const fila = [];

  // Junta frases curtas antes de mandar pra voz: prosódia natural
  const empurrar = (texto, ehPrimeira) => {
    pendente = pendente ? pendente + " " + texto : texto;
    const minimo = ehPrimeira ? MIN_FALA_PRIMEIRA : MIN_FALA_RESTO;
    if (pendente.length >= minimo) {
      for (const p of quebrarSeLonga(pendente)) fila.push(p);
      pendente = "";
    }
  };

  try {
    st.historico.push({ role: "user", content: falaDoCliente });

    const stream = anthropic.messages.stream({
      model: CLAUDE_MODEL,
      max_tokens: 110,
      system: PERSONA_STREAMS,
      messages: sanitizar(st.historico),
    });
    st.streamClaude = stream;

    stream.on("text", (delta) => {
      buffer += delta;
      completo += delta;
      let m;
      while ((m = buffer.match(/^([\s\S]*?[.!?…]+)(\s|$)/))) {
        empurrar(m[1].trim(), fila.length === 0);
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
          } else {
            console.log(`[cerebro] continua: "${trecho}"`);
          }
          await falarComMinhaVoz(ws, st, trecho, meuTurno);
        } else if (acabouTexto) {
          return;
        } else {
          await pausa(50);
        }
      }
      console.log("[cerebro] turno abortado — parei de falar");
    })();

    await stream.finalMessage();
    const resto = (pendente + " " + buffer).trim();
    if (resto) for (const p of quebrarSeLonga(resto)) fila.push(p);
    pendente = ""; buffer = "";
    acabouTexto = true;
    st.claudePensando = false;
    await consumidor;

    if (st.turno === meuTurno && completo.trim()) {
      st.historico.push({ role: "assistant", content: completo.trim() });
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

// ---------------- O TÚNEL ----------------
wssStreams.on("connection", (ws) => {
  const st = {
    streamSid: null, callSid: null, pacotes: 0, dg: null, dgPronto: false, fila: [],
    balde: "", historico: [], turno: 0, falando: false, claudePensando: false,
    marcasPendentes: 0, podeInterromperApos: 0, streamClaude: null,
    ultimoOuvido: Date.now(),
  };
  console.log("[streams] túnel aberto, aguardando áudio…");

  function abrirOuvido() {
    if (!DEEPGRAM_API_KEY) {
      console.error("[deepgram] DEEPGRAM_API_KEY não configurada!");
      return;
    }
    const url =
      "wss://api.deepgram.com/v1/listen" +
      "?encoding=mulaw&sample_rate=8000&channels=1" +
      "&language=pt-BR&model=nova-2" +
      "&punctuate=true&smart_format=true" +
      "&interim_results=true&endpointing=300";
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
      if (ev.type !== "Results") return;
      const texto = (ev.channel?.alternatives?.[0]?.transcript || "").trim();
      if (!texto) return;

      st.ultimoOuvido = Date.now();
      const estaFalando = st.falando || st.marcasPendentes > 0 || st.claudePensando;

      // ---- BARGE-IN ----
      if (estaFalando && texto.length >= BARGE_MIN_CHARS) {
        if (Date.now() > st.podeInterromperApos) {
          calarABoca(ws, st, `pessoa disse: "${texto}"`);
          st.balde = "";
        } else {
          console.log(`[barge-in] (segurei, ainda na carência) "${texto}"`);
        }
      }

      if (!ev.is_final) {
        console.log(`[ouvido] ouvindo… "${texto}"`);
        return;
      }

      console.log(`[ouvido] FINAL: "${texto}"${ev.speech_final ? "  <-- terminou" : ""}`);
      st.balde = (st.balde ? st.balde + " " : "") + texto;

      if (ev.speech_final) {
        const falaCompleta = st.balde.trim();
        st.balde = "";
        if (!falaCompleta) return;
        console.log(`[ouvido] >>> pessoa disse: "${falaCompleta}"`);
        pensarEResponder(ws, st, falaCompleta);
      }
    });

    dg.on("error", (e) => console.error("[deepgram] erro:", e?.message));
    dg.on("close", (code) => {
      st.dgPronto = false;
      console.log("[deepgram] ouvido desconectado (código", code + ")");
    });
  }

  // Vigia: avisa se o ouvido ficar mudo por muito tempo
  const vigia = setInterval(() => {
    const mudoHa = Math.round((Date.now() - st.ultimoOuvido) / 1000);
    if (mudoHa >= 12) {
      console.warn(
        `[vigia] ⚠️ ouvido mudo há ${mudoHa}s | dgPronto=${st.dgPronto} ` +
        `falando=${st.falando} marcas=${st.marcasPendentes} pacotes=${st.pacotes}`
      );
      st.ultimoOuvido = Date.now();
    }
  }, 6000);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.event === "start") {
      st.streamSid = msg.start?.streamSid || null;
      st.callSid = msg.start?.callSid || null;
      console.log("[streams] início — callSid:", st.callSid);
      abrirOuvido();
      st.turno++;
      st.falando = true;
      st.podeInterromperApos = Date.now() + 1500;
      falarComMinhaVoz(ws, st, SAUDACAO_STREAMS, st.turno);
      return;
    }

    if (msg.event === "media") {
      st.pacotes++;
      if (st.pacotes % 1000 === 0) console.log(`[streams] áudio fluindo… ${st.pacotes} pacotes`);
      const audio = Buffer.from(msg.media.payload, "base64");
      if (st.dgPronto) {
        try { st.dg.send(audio); } catch {}
      } else {
        st.fila.push(audio);
        if (st.fila.length > 500) st.fila.shift();
      }
      return;
    }

    if (msg.event === "mark") {
      st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
      if (st.marcasPendentes === 0 && !st.claudePensando) {
        st.falando = false;
        console.log("[streams] terminou de falar — escutando 👂");
      }
      return;
    }

    if (msg.event === "stop") {
      console.log(`[streams] fim do túnel — total de pacotes: ${st.pacotes}`);
      return;
    }
  });

  ws.on("close", () => {
    clearInterval(vigia);
    console.log("[streams] túnel fechado. pacotes:", st.pacotes);
    console.log("[conversa] turnos trocados:", st.historico.length);
    if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} }
    if (st.dg) { try { st.dg.close(); } catch {} }
  });
  ws.on("error", (e) => { clearInterval(vigia); console.error("[streams] erro no túnel:", e?.message); });
});

server.listen(PORT, () => {
  console.log(`[VozIA] motor de voz ouvindo na porta ${PORT}`);
  avisarFaltando();
});
