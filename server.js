/**
 * VozIA — Motor de Voz
 * ---------------------------------------------------------------------------
 * Servidor Node.js que conecta a Twilio Conversation Relay ao Claude (Anthropic),
 * lê campanhas/contatos/agentes do Supabase e grava as ligações de volta no banco.
 *
 * IMPORTANTE: NÃO coloque chaves neste arquivo. Todas as chaves vêm de
 * variáveis de ambiente, configuradas na Railway (veja .env.example).
 * ---------------------------------------------------------------------------
 */

import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
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

// Clientes (criados só se as chaves existirem, pra o deploy subir mesmo antes de tudo configurado)
const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;
const twilioClient =
  process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
    ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)
    : null;
const supabase =
  process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY
    ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)
    : null;

function avisarFaltando() {
  const faltando = [];
  if (!anthropic) faltando.push("ANTHROPIC_API_KEY");
  if (!twilioClient) faltando.push("TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN");
  if (!supabase) faltando.push("SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY");
  if (!PUBLIC_HOST) faltando.push("PUBLIC_HOST (ou RAILWAY_PUBLIC_DOMAIN)");
  if (!ELEVENLABS_VOICE_ID) faltando.push("ELEVENLABS_VOICE_ID");
  if (!TWILIO_FROM) faltando.push("TWILIO_FROM");
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

// Substitui marcadores comuns de nome ([nome], [NOME]...) pelo nome do contato
function preencherNome(texto = "", nome = "") {
  if (!nome) return texto;
  return texto.replace(/\[(nome|NOME|nome do contato|NOME DO CONTATO)\]/g, nome);
}

// Decide a voz: usa o voz_id do agente se parecer um ID real do ElevenLabs; senão usa a do .env
function resolverVoz(agente) {
  const v = agente && agente.voz_id ? String(agente.voz_id).trim() : "";
  if (/^[A-Za-z0-9]{18,}$/.test(v)) return v; // parece um Voice ID do ElevenLabs
  return ELEVENLABS_VOICE_ID;
}

// Garante que a lista de mensagens comece com "user" e alterne (exigência da API do Claude)
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

// Carrega campanha + agente + contato do Supabase
async function carregarContexto(campanhaId, contatoId) {
  const { data: campanha } = await supabase
    .from("campanhas")
    .select("*")
    .eq("id", campanhaId)
    .single();
  let agente = null,
    contato = null;
  if (campanha?.agente_id) {
    const r = await supabase.from("agentes").select("*").eq("id", campanha.agente_id).single();
    agente = r.data;
  }
  if (contatoId) {
    const r = await supabase.from("contatos").select("*").eq("id", contatoId).single();
    contato = r.data;
  }
  return { campanha, agente, contato };
}

// Monta o system prompt do Claude a partir do agente + contato
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

// Saúde (pra você abrir no navegador e ver que está no ar)
app.get("/", (req, res) => res.send("VozIA — motor de voz online ✅"));

// TwiML: a Twilio busca isto quando a ligação conecta
app.all("/twiml", async (req, res) => {
  try {
    const campanhaId = req.query.campanha_id || "";
    const contatoId = req.query.contato_id || "";
    let saudacao = "Olá, tudo bem? Você tem um minutinho?";
    let voice = ELEVENLABS_VOICE_ID;
    let language = "pt-BR";

    if (supabase && campanhaId) {
      const { agente, contato } = await carregarContexto(campanhaId, contatoId);
      if (agente?.saudacao_inicial) saudacao = preencherNome(agente.saudacao_inicial, contato?.nome);
      voice = resolverVoz(agente) || ELEVENLABS_VOICE_ID;
      language = agente?.idioma || "pt-BR";
    }

    const host = PUBLIC_HOST || req.headers.host;
    const wsUrl = `wss://${host}/ws`;

    const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="${escapeXml(wsUrl)}" ttsProvider="ElevenLabs" voice="${escapeXml(
      voice
    )}" transcriptionProvider="Deepgram" language="${escapeXml(language)}" welcomeGreeting="${escapeXml(
      saudacao
    )}" interruptible="speech">
      <Parameter name="campanha_id" value="${escapeXml(campanhaId)}"/>
      <Parameter name="contato_id" value="${escapeXml(contatoId)}"/>
    </ConversationRelay>
  </Connect>
</Response>`;
    res.type("text/xml").send(twiml);
  } catch (e) {
    console.error("[/twiml] erro:", e);
    res
      .type("text/xml")
      .send(
        `<?xml version="1.0" encoding="UTF-8"?><Response><Say language="pt-BR">Desculpe, ocorreu um erro.</Say><Hangup/></Response>`
      );
  }
});

// Discador: o painel (via Supabase Edge Function) chama isto para iniciar a campanha
app.post("/campanhas/iniciar", async (req, res) => {
  // autenticação simples por segredo compartilhado
  const auth = req.headers.authorization || "";
  if (!VOICE_BACKEND_SECRET || auth !== `Bearer ${VOICE_BACKEND_SECRET}`) {
    return res.status(401).json({ error: "não autorizado" });
  }
  if (!supabase || !twilioClient) {
    return res.status(500).json({ error: "servidor sem Supabase/Twilio configurados" });
  }
  const campanhaId = req.body?.campanha_id;
  if (!campanhaId) return res.status(400).json({ error: "campanha_id é obrigatório" });

  try {
    const { data: campanha } = await supabase
      .from("campanhas")
      .select("*")
      .eq("id", campanhaId)
      .single();
    if (!campanha) return res.status(404).json({ error: "campanha não encontrada" });

    // pega contatos que estão na fila (em lotes, pra não disparar tudo de uma vez)
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

    let dialed = 0;
    for (const item of itens) {
      const { data: contato } = await supabase
        .from("contatos")
        .select("*")
        .eq("id", item.contato_id)
        .single();
      if (!contato?.telefone) continue;

      const twimlUrl = `https://${host}/twiml?campanha_id=${encodeURIComponent(
        campanhaId
      )}&contato_id=${encodeURIComponent(item.contato_id)}`;

      try {
        const call = await twilioClient.calls.create({
          to: contato.telefone,
          from: TWILIO_FROM,
          url: twimlUrl,
          machineDetection: "Enable", // tenta detectar caixa postal
        });

        await supabase.from("ligacoes").insert({
          user_id: campanha.user_id,
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

// ----------------------- WebSocket (a conversa em si) -----------------------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const sessions = new Map(); // ws -> dados da sessão

wss.on("connection", (ws) => {
  const session = {
    callSid: null,
    campanhaId: null,
    contatoId: null,
    history: [], // mensagens enviadas ao Claude
    transcript: [], // transcrição salva no banco
    startedAt: Date.now(),
    currentStream: null,
    agente: null,
    contato: null,
    saudacao: "",
    systemPrompt: "",
  };
  sessions.set(ws, session);

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    // 1) Conversation Relay manda "setup" assim que conecta
    if (msg.type === "setup") {
      session.callSid = msg.callSid;
      const p = msg.customParameters || {};
      session.campanhaId = p.campanha_id || "";
      session.contatoId = p.contato_id || "";

      let saudacao = "Olá!";
      if (supabase && session.campanhaId) {
        try {
          const { agente, contato } = await carregarContexto(session.campanhaId, session.contatoId);
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
      // a saudação já foi falada pela Twilio (welcomeGreeting); registramos na transcrição
      session.transcript.push(`Assistente: ${saudacao}`);
      console.log("[setup] callSid:", session.callSid, "campanha:", session.campanhaId);
      return;
    }

    // 2) "prompt" = o que o cliente falou (já transcrito)
    if (msg.type === "prompt") {
      const fala = msg.voicePrompt || "";
      if (!fala.trim()) return;

      // se há uma resposta em andamento, cancela (lida com fala sobreposta / interrupção)
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

    // 3) "interrupt" = o cliente interrompeu a fala do robô
    if (msg.type === "interrupt") {
      if (session.currentStream) {
        try {
          session.currentStream.abort();
        } catch {}
        session.currentStream = null;
      }
      return;
    }

    // 4) "error" = a Conversation Relay reportou um erro
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

// Salva a ligação no Supabase quando ela termina
async function finalizarLigacao(session) {
  if (!supabase || !session.callSid) return;
  const duracao = Math.round((Date.now() - session.startedAt) / 1000);
  const transcricao = session.transcript.join("\n");
  const houveConversa = session.history.length > 0;

  // extrai resultado/sentimento/nota com o Claude (best-effort, não quebra se falhar)
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

// ----------------------- Start -----------------------
server.listen(PORT, () => {
  console.log(`[VozIA] motor de voz ouvindo na porta ${PORT}`);
  avisarFaltando();
});
