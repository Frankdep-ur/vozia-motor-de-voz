/**
 * VozIA — Motor de Voz  (versão Lovable Cloud)
 * ---------------------------------------------------------------------------
 * FASE 4: o motor novo (Media Streams) agora FALA com a voz clonada do Frank.
 * ---------------------------------------------------------------------------
 */

import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import Anthropic from "@anthropic-ai/sdk";
import twilio from "twilio";
import { createClient } from "@supabase/supabase-js";

// ----------------------- Configuração (variáveis de ambiente) -----------------------
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
  if (!twilioClient) {
    return res.status(500).json({ error: "Twilio não configurado" });
  }
  const sb = await getSupabaseLogado();
  if (!sb) {
    return res.status(500).json({ error: "não foi possível acessar o banco (login Supabase falhou)" });
  }
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
    callSid: null,
    campanhaId: null,
    contatoId: null,
    history: [],
    transcript: [],
    startedAt: Date.now(),
    currentStream: null,
    agente: null,
    contato: null,
    saudacao: "",
    systemPrompt: "",
    supabase: null,
    userId: null,
  };
  sessions.set(ws, session);

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

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
            sb.client,
            session.campanhaId,
            session.contatoId
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
        try {
          session.currentStream.abort();
        } catch {}
        session.currentStream = null;
      }

      session.transcript.push(`Cliente: ${fala}`);
      session.history.push({ role: "user", content: fala });

      if (!anthropic) {
        ws.send(
          JSON.stringify({
            type: "text",
            token: "Desculpe, estou com um problema técnico no momento.",
            last: true,
          })
        );
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
          try {
            ws.send(JSON.stringify({ type: "text", token: delta, last: false }));
          } catch {}
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
        try {
          session.currentStream.abort();
        } catch {}
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

  let resultado = null,
    sentimento = null,
    nota = null;
  if (anthropic && houveConversa) {
    try {
      const r = await anthropic.messages.create({
        model: CLAUDE_MODEL,
        max_tokens: 200,
        messages: [
          {
            role: "user",
            content: `Abaixo está a transcrição de uma ligação. Responda APENAS com um JSON válido, sem texto extra e sem markdown, no formato exato: {"resultado":"resumo curto do que aconteceu","sentimento":"positivo|neutro|negativo","nota": número de 1 a 10 que o cliente deu, ou null}. Transcrição:\n\n${transcricao}`,
          },
        ],
      });
      let txt = (r.content?.[0]?.text || "")
        .trim()
        .replace(/^```(json)?/i, "")
        .replace(/```$/, "")
        .trim();
      const obj = JSON.parse(txt);
      resultado = obj.resultado ?? null;
      sentimento = ["positivo", "neutro", "negativo"].includes(obj.sentimento)
        ? obj.sentimento
        : null;
      nota = typeof obj.nota === "number" ? obj.nota : null;
    } catch (e) {
      console.error("[finalizar] erro ao extrair resumo:", e?.message);
    }
  }

  try {
    await supabase
      .from("ligacoes")
      .update({
        status: houveConversa ? "atendida" : "sem_resposta",
        duracao_segundos: duracao,
        transcricao,
        resultado,
        sentimento,
        nota,
        finalizada_em: new Date().toISOString(),
      })
      .eq("twilio_call_sid", session.callSid);

    if (session.campanhaId && session.contatoId) {
      await supabase
        .from("campanha_contatos")
        .update({
          status: houveConversa ? "concluida" : "sem_resposta",
          atualizado_em: new Date().toISOString(),
        })
        .eq("campanha_id", session.campanhaId)
        .eq("contato_id", session.contatoId);
    }
    console.log("[finalizar] ligação salva:", session.callSid, "duração:", duracao + "s");
  } catch (e) {
    console.error("[finalizar] erro ao salvar:", e?.message);
  }
}

// ============================================================================
// =========  MOTOR NOVO (Twilio Media Streams) — FASES 2, 3 e 4  =============
// ============================================================================

const ELEVENLABS_VOICE_ID_CLONE = process.env.ELEVENLABS_VOICE_ID_CLONE || "";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_MODEL = process.env.ELEVENLABS_MODEL || "eleven_flash_v2_5";

// Frase de teste da Fase 4 — falada com a SUA voz clonada
const FRASE_TESTE_FASE4 =
  "Oi Frank! Se você está me ouvindo agora, essa é a sua própria voz clonada, " +
  "saindo direto do motor da VozIA. A fase quatro está funcionando. " +
  "Agora falta só ligar o cérebro, e a gente vai conversar de verdade. " +
  "Pode falar alguma coisa, que eu continuo escutando, e depois pode desligar!";

app.get("/streams/teste", async (req, res) => {
  const senha = req.query.senha || "";
  if (!VOICE_BACKEND_SECRET || senha !== VOICE_BACKEND_SECRET) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!twilioClient) return res.status(500).json({ error: "Twilio não configurado" });

  const digitos = String(req.query.para || "").replace(/\D/g, "");
  if (digitos.length < 12) {
    return res
      .status(400)
      .json({ error: "use ?para=5518999999999 (país + DDD + número, só dígitos)" });
  }
  const para = "+" + digitos;
  const host = PUBLIC_HOST || req.headers.host;

  try {
    const call = await twilioClient.calls.create({
      to: para,
      from: TWILIO_FROM,
      url: `https://${host}/twiml-streams`,
    });
    console.log("[streams/teste] ligando para", para, "callSid:", call.sid);
    res.json({ ok: true, ligando_para: para, callSid: call.sid });
  } catch (e) {
    console.error("[streams/teste] erro:", e?.message);
    res.status(500).json({ error: e?.message || "erro ao ligar" });
  }
});

// --- TwiML do motor novo: abre o túnel direto, sem voz robótica ---
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

// ----------------------------------------------------------------------------
// A BOCA: pede o áudio pra ElevenLabs (sua voz clonada, formato de telefone)
// e devolve pelo túnel pra pessoa ouvir.
// ----------------------------------------------------------------------------
async function falarComMinhaVoz(ws, streamSid, texto) {
  if (!ELEVENLABS_API_KEY) {
    console.error("[boca] ELEVENLABS_API_KEY não configurada!");
    return;
  }
  if (!ELEVENLABS_VOICE_ID_CLONE) {
    console.error("[boca] ELEVENLABS_VOICE_ID_CLONE não configurada!");
    return;
  }
  if (!streamSid) {
    console.error("[boca] sem streamSid — não dá pra enviar áudio");
    return;
  }

  const inicio = Date.now();
  const url =
    `https://api.elevenlabs.io/v1/text-to-speech/${ELEVENLABS_VOICE_ID_CLONE}/stream` +
    `?output_format=ulaw_8000`;

  console.log(`[boca] pedindo áudio pra ElevenLabs (voz ${ELEVENLABS_VOICE_ID_CLONE})…`);

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        text: texto,
        model_id: ELEVENLABS_MODEL,
        language_code: "pt",
        voice_settings: { stability: 0.5, similarity_boost: 0.8 },
      }),
    });

    if (!resp.ok) {
      const erro = await resp.text();
      console.error("[boca] ElevenLabs recusou:", resp.status, erro.slice(0, 400));
      return;
    }

    const audio = Buffer.from(await resp.arrayBuffer());
    const demorou = Date.now() - inicio;
    console.log(`[boca] áudio pronto: ${audio.length} bytes em ${demorou}ms`);

    // Manda em pedacinhos de 80ms (a Twilio guarda e toca na velocidade certa)
    const PEDACO = 640;
    let enviados = 0;
    for (let i = 0; i < audio.length; i += PEDACO) {
      const parte = audio.subarray(i, i + PEDACO);
      ws.send(
        JSON.stringify({
          event: "media",
          streamSid,
          media: { payload: parte.toString("base64") },
        })
      );
      enviados++;
    }
    ws.send(JSON.stringify({ event: "mark", streamSid, mark: { name: "fim-da-fala" } }));
    console.log(`[boca] ${enviados} pedaços enviados — o telefone está falando ✅`);
  } catch (e) {
    console.error("[boca] erro:", e?.message || e);
  }
}

// --- O túnel: OUVIDO (Deepgram) + BOCA (ElevenLabs) ---
wssStreams.on("connection", (ws) => {
  const st = { streamSid: null, callSid: null, pacotes: 0, dg: null, dgPronto: false, fila: [] };
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
      const texto = ev.channel?.alternatives?.[0]?.transcript || "";
      if (!texto.trim()) return;
      if (ev.is_final) {
        console.log(`[ouvido] FINAL: "${texto}"${ev.speech_final ? "  <-- terminou de falar" : ""}`);
      } else {
        console.log(`[ouvido] ouvindo… "${texto}"`);
      }
    });

    dg.on("error", (e) => console.error("[deepgram] erro:", e?.message));
    dg.on("close", (code) => {
      st.dgPronto = false;
      console.log("[deepgram] ouvido desconectado (código", code + ")");
    });
  }

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.event === "start") {
      st.streamSid = msg.start?.streamSid || null;
      st.callSid = msg.start?.callSid || null;
      console.log("[streams] início — callSid:", st.callSid);
      abrirOuvido();
      falarComMinhaVoz(ws, st.streamSid, FRASE_TESTE_FASE4);
      return;
    }

    if (msg.event === "media") {
      st.pacotes++;
      if (st.pacotes % 200 === 0) console.log(`[streams] áudio fluindo… ${st.pacotes} pacotes`);
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
      console.log("[streams] a fala terminou de tocar:", msg.mark?.name);
      return;
    }

    if (msg.event === "stop") {
      console.log(`[streams] fim do túnel — total de pacotes: ${st.pacotes}`);
      return;
    }
  });

  ws.on("close", () => {
    console.log("[streams] túnel fechado. pacotes:", st.pacotes);
    if (st.dg) { try { st.dg.close(); } catch {} }
  });
  ws.on("error", (e) => console.error("[streams] erro no túnel:", e?.message));
});

server.listen(PORT, () => {
  console.log(`[VozIA] motor de voz ouvindo na porta ${PORT}`);
  avisarFaltando();
});
