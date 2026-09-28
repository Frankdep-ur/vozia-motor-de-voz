/**
 * VozIA — Motor de Voz  (versão Lovable Cloud)
 * ---------------------------------------------------------------------------
 * FASE 12.3 — AJUSTES DO TESTE DE 28/09 (13h)
 *   Abertura: se ouviu voz mas o texto do "Alô?" ainda não chegou, espera o texto
 *     (o Deepgram demora ~1s) — no teste o Carlos começou 0,7s depois e atropelou
 *   Regra de ouro 5: fala pronta do roteiro (explicação do desconto) sai inteira
 * ---------------------------------------------------------------------------
 * FASE 12.2 — LIÇÕES DO PILOTO DE 28/09 (30 contatos)
 *   Espera o "alô" antes de falar (não atropela quem atende; máquina é pega
 *     antes do Carlos falar). Ninguém falou em 1,5s → começa a saudação.
 *   Assistente de chamadas do celular ("se você disser seu nome e o motivo da
 *     ligação…"): o Carlos se apresenta numa frase e espera calado. Se a pessoa
 *     atender, repete a saudação; se "não está disponível", desliga e tenta depois
 *   Recado da operadora ("vamos entregar o seu recado assim que o celular estiver
 *     disponível") e caixa postal: desliga e tenta de novo depois — não conta
 *     mais como "atendida"
 *   "Permaneça na linha" / "aguarde": espera em silêncio (sem "cortou aqui")
 *   Silêncio total depois da saudação: "Alô? Tá me ouvindo?" e desliga
 *   Nunca fala marcador entre colchetes ("[AGUARDANDO]" foi falado no piloto)
 *   Nome: não some mais no meio da frase ("O está aí?") e erro de digitação
 *     do nome é corrigido ("Adenar" → "Ademar")
 *   Transcrição guarda as falas interrompidas do Carlos
 *   Ligação caiu → para de pensar e de falar na hora
 *   Resultado claro no banco (caixa postal, recado, assistente, silêncio)
 *   "Não me liga mais" → contato vira "Não atender" (nenhuma campanha liga de novo)
 *   Depois da saudação espera 5,5s antes de re-perguntar (a pessoa ainda está entendendo)
 * ---------------------------------------------------------------------------
 * FASE 12.1 — DISCADOR AUTOMÁTICO
 *   Campanha: clicou Iniciar uma vez, ela vai até o fim sozinha (3 por vez)
 *   Nunca liga de novo pra quem já conversou com o Carlos nesta campanha
 *   Tentativas contadas pelo histórico real (o botão Iniciar zera no painel)
 *   Não atendeu → tenta de novo depois de 30 min (até o máximo da campanha)
 *   Caixa postal → desliga na hora e conta como "não atendeu" (tenta de novo)
 *   Respeita Pausar, "Não atender" e o horário (seg–sex 8h–20h, sáb 8h–14h)
 *   Campanha de TESTE (até 3 contatos, ex.: só o seu número) funciona como antes:
 *     Iniciar liga na hora, a qualquer hora, e dá pra repetir com Pausar → Iniciar
 *   Campanha de verdade (4+ contatos) com Iniciar fora do horário: não liga na hora,
 *     espera e começa sozinha quando o horário abrir
 *   Erro da CONTA Twilio → pausa a campanha (não queima a lista como "falhou")
 *   "Agendar para" do painel agora funciona
 * ---------------------------------------------------------------------------
 * FASE 12
 *   Ouvido:   eco controlado pelo TEMPO do áudio (não pelo texto): "Sim" + "Sim"
 *             seguidos não somem mais, e o final repetido não interrompe o Carlos
 *             resposta dada no fim da pergunta fica guardada e é respondida
 *             resposta curta e valor da conta fecham mais rápido
 *             "voz detectada" + atraso do Deepgram aparecem no log
 *             se o Deepgram cair no meio da ligação, reconecta sozinho
 *   Vigia:    se a resposta não chega, repete a pergunta em poucos segundos
 *   Turnos:   turno antigo não bagunça mais o turno novo (sem "surdez" presa)
 *   Cérebro:  se o Claude travar ou der erro, o Carlos pede pra repetir
 *   WhatsApp: o próprio motor converte o número falado em dígitos, confere
 *             DDD + 9 e lê de volta — o Claude não mexe mais no número
 *   Log:      cada linha com [final do callSid + segundos da ligação]
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
const MOTOR_PADRAO = (process.env.MOTOR_PADRAO || "streams").toLowerCase();
const PERMITIR_TESTE = process.env.PERMITIR_TESTE === "1";
const DETECTAR_SECRETARIA = process.env.DETECTAR_SECRETARIA === "1";
const GRAVAR_LIGACOES = process.env.GRAVAR_LIGACOES === "1";
const VARRER_MINUTOS = parseInt(process.env.VARRER_MINUTOS || "10", 10);
const MARGEM_CALLBACK_MS = parseInt(process.env.MARGEM_CALLBACK_MS || "4000", 10);

// Discador automático (horário de Brasília)
const HORA_INICIO = parseFloat(process.env.HORA_INICIO || "8");          // seg–sáb
const HORA_FIM = parseFloat(process.env.HORA_FIM || "20");               // seg–sex
const HORA_FIM_SABADO = parseFloat(process.env.HORA_FIM_SABADO || "14");
const LIGAR_DOMINGO = process.env.LIGAR_DOMINGO === "1";
const RETENTAR_APOS_MIN = parseFloat(process.env.RETENTAR_APOS_MIN || "30");
const CAMPANHA_CHECA_S = parseFloat(process.env.CAMPANHA_CHECA_S || "30");
const PAUSA_ENTRE_LIGACOES_MS = parseInt(process.env.PAUSA_ENTRE_LIGACOES_MS || "3000", 10);
// Caixa postal / recado da operadora / assistente de chamadas: desliga e conta como
// "não atendeu" (volta pra fila). JANELA 0 = vale a ligação inteira.
const CAIXA_POSTAL = process.env.CAIXA_POSTAL !== "0";
const CAIXA_POSTAL_JANELA_S = parseFloat(process.env.CAIXA_POSTAL_JANELA_S || "0");

// Abertura: espera o "alô" da pessoa antes da saudação
const ESPERAR_ALO = process.env.ESPERAR_ALO !== "0";
const ALO_SILENCIO_MS = parseInt(process.env.ALO_SILENCIO_MS || "1500", 10); // ninguém falou → começa
const ALO_PAUSA_MS = parseInt(process.env.ALO_PAUSA_MS || "500", 10);       // falou "alô" e parou → começa
// Ouviu voz mas o texto ainda não chegou (o Deepgram demora ~1s pra mandar o "Alô?"): espera o texto
const ALO_VOZ_SEM_TEXTO_MS = parseInt(process.env.ALO_VOZ_SEM_TEXTO_MS || "1600", 10);
const ALO_MAX_MS = parseInt(process.env.ALO_MAX_MS || "6000", 10);          // limite da espera
// Espera ("permaneça na linha", "aguarde") e assistente de chamadas
const ESPERA_MAX_MS = parseInt(process.env.ESPERA_MAX_MS || "45000", 10);
const TRIAGEM_MAX_MS = parseInt(process.env.TRIAGEM_MAX_MS || "60000", 10);
const FRASE_MOTIVO_TRIAGEM = process.env.FRASE_MOTIVO_TRIAGEM || "Estou ligando sobre desconto na conta de luz.";
// Silêncio total (nunca ouviu voz): depois da re-pergunta curta, desliga
const FRASE_ALO = "Alô? Tá me ouvindo?";
const SILENCIO_SEM_VOZ_MS = parseInt(process.env.SILENCIO_SEM_VOZ_MS || "4000", 10);

const BARGE_MIN_CHARS = parseInt(process.env.BARGE_MIN_CHARS || "14", 10);
const MIN_FALA_PRIMEIRA = parseInt(process.env.MIN_FALA_PRIMEIRA || "12", 10);
const MIN_FALA_RESTO = parseInt(process.env.MIN_FALA_RESTO || "45", 10);
const MAX_FALA = parseInt(process.env.MAX_FALA || "110", 10);
const MIN_PEDACO = parseInt(process.env.MIN_PEDACO || "22", 10);
const DG_ENDPOINTING = parseInt(process.env.DG_ENDPOINTING || "300", 10);
const SILENCIO_PADRAO_MS = parseInt(process.env.SILENCIO_MS || "9000", 10);
const FLUSH_MS = parseInt(process.env.FLUSH_MS || "900", 10);
const FLUSH_REPETIDO_MS = parseInt(process.env.FLUSH_REPETIDO_MS || "500", 10);
const FLUSH_CURTO_MS = parseInt(process.env.FLUSH_CURTO_MS || "250", 10);
const FLUSH_VALOR_MS = parseInt(process.env.FLUSH_VALOR_MS || "400", 10);
const ECO_MS = parseInt(process.env.ECO_MS || "5000", 10);
// Resposta dada nos últimos X ms da pergunta do Carlos fica guardada (não é jogada fora)
const JANELA_RESPOSTA_MS = parseInt(process.env.JANELA_RESPOSTA_MS || "2500", 10);

// Vigia: re-pergunta rápida quando a resposta não chega
const REPERGUNTAR = process.env.REPERGUNTAR !== "0";
const REPERGUNTA_MS = parseInt(process.env.REPERGUNTA_MS || "5000", 10);
const REPERGUNTA_VOZ_MS = parseInt(process.env.REPERGUNTA_VOZ_MS || "3000", 10);
const REPERGUNTA_SAUDACAO_MS = parseInt(process.env.REPERGUNTA_SAUDACAO_MS || "5500", 10);
const FRASE_REPERGUNTA = "Desculpa, acho que cortou aqui.";

// Cérebro: tempo máximo até a primeira palavra do Claude
const CLAUDE_TIMEOUT_MS = parseInt(process.env.CLAUDE_TIMEOUT_MS || "7000", 10);
const FRASE_FALHA = "Desculpa, falhou aqui. Pode repetir, por favor?";

// Ouvido
const DG_URL = process.env.DG_URL || "wss://api.deepgram.com/v1/listen";
const DG_MODEL = process.env.DG_MODEL || "nova-3";
const DG_SMART_FORMAT = process.env.DG_SMART_FORMAT === "1";
const DG_KEYTERMS = (process.env.DG_KEYTERMS ||
  "cem,duzentos,trezentos,quatrocentos,quinhentos,seiscentos,setecentos,oitocentos," +
  "novecentos,mil,reais,Invite,Invite Energy,WhatsApp,desconto,conta de luz,usina solar")
  .split(",").map((s) => s.trim()).filter(Boolean).slice(0, 100);

// WhatsApp — modo ditado
const FLUSH_DITADO_MS = parseInt(process.env.FLUSH_DITADO_MS || "2800", 10);
const DITADO_JANELA_MS = parseInt(process.env.DITADO_JANELA_MS || "30000", 10);
const MAX_TENTATIVAS_NUMERO = parseInt(process.env.MAX_TENTATIVAS_NUMERO || "2", 10);
const SAIDA_SEGURA_NUMERO =
  "Sem problema, a equipe te liga nesse número pra confirmar o WhatsApp. Valeu e até mais!";

// Roteiro
const UMA_PERGUNTA = process.env.UMA_PERGUNTA !== "0";
const MAX_NOME_POR_TURNO = parseInt(process.env.MAX_NOME_POR_TURNO || "1", 10);
const MAX_USOS_NOME = parseInt(process.env.MAX_USOS_NOME || "0", 10);   // 0 = sem limite

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_USER_EMAIL = process.env.SUPABASE_USER_EMAIL || "";
const SUPABASE_USER_PASSWORD = process.env.SUPABASE_USER_PASSWORD || "";

const ELEVENLABS_VOICE_ID_CLONE = process.env.ELEVENLABS_VOICE_ID_CLONE || "";
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_MODEL = process.env.ELEVENLABS_MODEL || "eleven_flash_v2_5";
const ELEVENLABS_URL = (process.env.ELEVENLABS_URL || "https://api.elevenlabs.io").replace(/\/+$/, "");

const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }) : null;
const twilioClient =
  process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
    ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN) : null;

const pausa = (ms) => new Promise((r) => setTimeout(r, ms));
// O SDK do Claude não preenche e.name ("Error"): a checagem certa é pela classe.
function ehAbortado(e) {
  return !!e && ((Anthropic.APIUserAbortError && e instanceof Anthropic.APIUserAbortError) ||
    e.name === "AbortError" || e.name === "APIUserAbortError");
}

// ----------------------- Log por ligação -----------------------
// Cada linha sai com [final do callSid + segundos desde o início da ligação].
// Assim dá pra separar ligações simultâneas e ler a ordem certa na Railway.
function marca(st) {
  if (!st || !st.tag) return "";
  const s = ((Date.now() - (st.iniciadoEm || Date.now())) / 1000).toFixed(1);
  return `[${st.tag} ${s}s] `;
}
function L(st, msg) { console.log(marca(st) + msg); }
function LW(st, msg) { console.warn(marca(st) + msg); }
function LE(st, msg) { console.error(marca(st) + msg); }

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
  if (!VOICE_BACKEND_SECRET) console.warn("[VozIA] ⚠️ VOICE_BACKEND_SECRET vazia — discador e callback desprotegidos!");
  console.log(
    `[VozIA] motor: ${MOTOR_PADRAO} | rota de teste: ${PERMITIR_TESTE ? "ABERTA ⚠️" : "fechada 🔒"} ` +
    `| secretária: ${DETECTAR_SECRETARIA ? "detectar" : "off"} | gravação: ${GRAVAR_LIGACOES ? "ON ⚠️" : "off"} | FASE 12.3`
  );
  console.log(
    `[VozIA] ouvido: ${DG_MODEL} | números em ${DG_SMART_FORMAT ? "algarismos" : "palavras"} ` +
    `| termos-chave: ${DG_MODEL.startsWith("nova-3") ? DG_KEYTERMS.length : "n/a (só nova-3)"}`
  );
  console.log(
    `[VozIA] whatsapp: modo ditado ${FLUSH_DITADO_MS}ms, máx ${MAX_TENTATIVAS_NUMERO} tentativas, número lido pelo motor ` +
    `| nome: ${MAX_NOME_POR_TURNO} por resposta, ${MAX_USOS_NOME > 0 ? MAX_USOS_NOME + " por ligação" : "sem limite na ligação"} ` +
    `| uma pergunta: ${UMA_PERGUNTA ? "ON" : "off"}`
  );
  console.log(
    `[VozIA] flush ${FLUSH_MS} · repetido ${FLUSH_REPETIDO_MS} · curto ${FLUSH_CURTO_MS} · valor ${FLUSH_VALOR_MS}ms ` +
    `| fala ${MIN_FALA_PRIMEIRA}/${MIN_FALA_RESTO}/${MAX_FALA} · pedaço mín ${MIN_PEDACO} | simultâneas ${MAX_CONCURRENT_CALLS}`
  );
  console.log(
    `[VozIA] re-pergunta: ${REPERGUNTAR ? `ON (${REPERGUNTA_MS}ms, ou ${REPERGUNTA_VOZ_MS}ms se ouviu voz)` : "off"} ` +
    `| resposta no fim da pergunta: guarda ${JANELA_RESPOSTA_MS}ms | cérebro: limite ${CLAUDE_TIMEOUT_MS}ms pra começar a responder`
  );
  console.log(
    `[VozIA] discador: automático, ${MAX_CONCURRENT_CALLS} por vez | horário ${descricaoHorario()} (Brasília) ` +
    `| não atendeu: tenta de novo após ${RETENTAR_APOS_MIN} min | caixa postal: ${CAIXA_POSTAL ? "desliga e tenta depois" : "off"} ` +
    `| agora: ${dentroDoHorario() ? "DENTRO do horário" : "fora do horário"} | FASE 12.3`
  );
  console.log(
    `[VozIA] abertura: ${ESPERAR_ALO ? `espera o alô (silêncio ${ALO_SILENCIO_MS}ms, pausa ${ALO_PAUSA_MS}ms, voz sem texto ${ALO_VOZ_SEM_TEXTO_MS}ms)` : "fala na hora"} ` +
    `| assistente de chamadas: se apresenta e espera até ${Math.round(TRIAGEM_MAX_MS / 1000)}s ` +
    `| "aguarde": espera calado até ${Math.round(ESPERA_MAX_MS / 1000)}s | FASE 12.3`
  );
}

// ----------------------- Utilidades -----------------------
function escapeXml(s = "") {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}
// Primeiro nome. Nome "grudado" suspeito (mais de 14 letras) não é falado.
function primeiroNome(nome = "") {
  const p = String(nome).trim().split(/\s+/)[0] || "";
  if (!p || p.length > 14 || /\d/.test(p)) return "";
  return p.charAt(0).toUpperCase() + p.slice(1).toLowerCase();
}
function resolverVoz(agente) {
  const v = agente && agente.voz_id ? String(agente.voz_id).trim() : "";
  if (/^[A-Za-z0-9]{18,}$/.test(v)) return v;
  return ELEVENLABS_VOICE_ID_CLONE || ELEVENLABS_VOICE_ID;
}
function entre(v, min, max, padrao) {
  const n = parseFloat(v);
  if (!isFinite(n)) return padrao;
  return Math.min(max, Math.max(min, n));
}
function resolverVozSettings(agente) {
  return {
    stability: entre(agente?.voz_estabilidade, 0, 1, 0.4),
    similarity_boost: entre(agente?.voz_similaridade, 0, 1, 0.8),
    style: entre(agente?.voz_estilo, 0, 1, 0.45),
  };
}
function configVoz(agente) {
  return {
    vozId: resolverVoz(agente),
    velocidade: entre(agente?.velocidade_fala, 0.7, 1.2, 1.0),
    settings: resolverVozSettings(agente),
  };
}
// Saudação com [nome] → primeiro nome. Sem nome, limpa a pontuação que sobra.
function textoSaudacao(agente, contato) {
  const nome = primeiroNome(contato?.nome || "");
  let t = String(agente?.saudacao_inicial || "Olá, tudo bem? Você tem um minutinho?");
  t = t.replace(/\[(nome|NOME|Nome|nome do contato|NOME DO CONTATO)\]/g, nome);
  t = t.replace(/,\s*([!?.])/g, "$1").replace(/\s+([,!?.])/g, "$1").replace(/\s{2,}/g, " ").trim();
  return t;
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
function juntarTexto(a, b) {
  return [a, b].map((x) => String(x || "").trim()).filter(Boolean).join(" ").replace(/\s{2,}/g, " ");
}

// Cola pedacinhos no vizinho (acaba o "de usina" / "solar,")
function juntarRabos(partes, min = MIN_PEDACO) {
  const out = [];
  for (const p of partes) {
    if (out.length && p.length < min) out[out.length - 1] += " " + p;
    else out.push(p);
  }
  if (out.length > 1 && out[0].length < min) { out[1] = out[0] + " " + out[1]; out.shift(); }
  return out;
}

function quebrarSeLonga(texto, limite = MAX_FALA) {
  if (texto.length <= limite) return [texto];
  const porVirgula = [];
  let atual = "";
  for (const pedaco of texto.split(/,\s*/)) {
    const cand = atual ? atual + ", " + pedaco : pedaco;
    if (cand.length > limite && atual) {
      porVirgula.push(atual.endsWith(",") ? atual : atual + ","); atual = pedaco;
    } else atual = cand;
  }
  if (atual) porVirgula.push(atual);
  const final = [];
  for (const p of porVirgula) {
    if (p.length <= limite) { final.push(p); continue; }
    let linha = "";
    for (const palavra of p.split(/\s+/)) {
      if ((linha + " " + palavra).trim().length > limite && linha) {
        final.push(linha.trim()); linha = palavra;
      } else linha = (linha + " " + palavra).trim();
    }
    if (linha) final.push(linha.trim());
  }
  return juntarRabos(final.filter(Boolean));
}

// ----- Respostas curtas (fecham rápido: 250ms) -----
const RESPOSTAS_CURTAS = new Set([
  "sim", "nao", "pode", "ok", "okay", "claro", "isso", "exato", "exatamente",
  "certo", "ta", "beleza", "quero", "tenho", "e", "faz", "entendi", "uhum", "aham",
  "correto", "perfeito", "entendo", "verdade", "sei", "sabia", "com", "certeza",
  "manda", "mandar", "ja", "ainda", "legal", "otimo", "alo", "quem", "pronto",
  "positivo", "negativo", "recebo", "pago", "topo", "bora", "fechado", "show",
  "boa", "tranquilo", "blz", "conheco", "conhecia", "gostaria", "senhor", "senhora",
  // FASE 12
  "estou", "to", "tou", "aqui", "eu", "mesmo", "ser", "ah", "oi", "hum", "hm", "ha",
  "como", "que", "assim", "fala", "diga", "o", "a", "meu", "minha", "esse", "essa", "nesse",
  "numero", "whatsapp", "zap", "sentido", "falei", "disse", "bom", "valeu", "obrigado",
  "obrigada", "tchau", "nada", "interesse", "agora", "depois", "liga", "ligar", "ocupado",
  "ocupada", "dirigindo", "trabalhando", "repete", "repetir", "onde", "de", "qual", "quanto",
  "ne", "la", "tambem", "talvez", "acho", "concordo", "entendido", "mande",
]);
function normalizar(t) {
  return String(t).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
}
function terminou(texto) { return /[.!?]$/.test(String(texto).trim()); }
function ehRespostaCurta(texto) {
  if (!terminou(texto)) return false;
  const palavras = normalizar(texto).split(" ").filter(Boolean);
  if (palavras.length === 0 || palavras.length > 4) return false;
  return palavras.every((p) => RESPOSTAS_CURTAS.has(p));
}
// Valor da conta: "quinhentos", "uns trezentos", "quatrocentos reais"
const FIM_DE_VALOR = /\b(reais|real|conto|contos|pila|cem|duzentos|trezentos|quatrocentos|quinhentos|seiscentos|setecentos|oitocentos|novecentos|mil|vinte|trinta|quarenta|cinquenta|sessenta|setenta|oitenta|noventa)$/;
function ehValorCompleto(texto) {
  if (!terminou(texto)) return false;
  const n = normalizar(texto);
  if (!n) return false;
  const qtd = n.split(" ").length;
  // "em quatrocentos e cinquenta e dois reais" (7 palavras) também é valor completo
  if (/\b(reais|real|conto|contos|pila)$/.test(n)) return qtd <= 10;
  if (qtd > 6) return false;
  return FIM_DE_VALOR.test(n) || /\d$/.test(n);
}

// Máquinas que atendem no lugar da pessoa (piloto de 28/09). Só frases que gente de
// verdade não fala numa ligação — por isso valem a ligação inteira.
const RE_CAIXA_POSTAL = new RegExp([
  "\\bcaixa postal\\b", "\\bcaixa de (mensagem|mensagens|recado|recados)\\b",
  "\\bvoice ?mail\\b", "\\bencaminhada (para|ao|a) (o |a )?(caixa|correio de voz)\\b", "\\bcorreio de voz\\b",
  "\\b(apos|depois do|ao ouvir o) (o )?(sinal|bip|bipe)\\b", "\\baguarde o (sinal|bip|bipe)\\b",
  "\\bsujeit[ao] a (cobranca|tarifacao)\\b",
  "\\b(deixe|grave) (a |o )?(sua |seu |uma |um )?(mensagem|recado)\\b",
  "\\bdeixa (seu|sua|um|uma) (mensagem|recado)\\b",
  "\\bfora da area de cobertura\\b", "\\bdesligado ou fora\\b",
  "\\bprogramado para nao receber\\b",
  "\\bnumero (que voce ligou|chamado|discado) (nao existe|esta desligado|nao esta disponivel)\\b",
].join("|"));
// Serviço de recado da operadora: "Vamos entregar o seu recado assim que o celular estiver disponível"
const RE_RECADO = /\b(vamos entregar o seu|entrego o seu recado|entregar o seu recado|assim que o (celular|telefone|aparelho) estiver|quando o (celular|telefone|aparelho) estiver disponivel)\b/;
// Assistente de chamadas do celular: fim da triagem sem a pessoa
const RE_INDISPONIVEL = /\b(esta pessoa nao esta disponivel|deixe outra mensagem)\b/;
// Assistente de chamadas pedindo nome e motivo
const RE_TRIAGEM = /\b(seu nome e o motivo|poderei ver se (esta|essa) pessoa|servico de triagem|triagem de chamadas|esta usando (o |um )?(bixby|assistente))\b/;
// Pedido pra esperar (assistente ou gente de verdade chamando alguém)
const RE_ESPERA = /\b(permaneca na linha|aguarde (na linha|um (momento|instante|minuto|minutinho|pouquinho))|aguarda (um|so) (pouco|pouquinho|minutinho|instante|momento)|so um (minutinho|momento|instante|segundinho)|um momentinho|vou chamar (ele|ela|o|a))\b/;

// O que atendeu? "caixa" | "recado" | "indisponivel" | "triagem" | "espera" | ""
function tipoDeMaquina(texto) {
  const n = normalizar(texto);
  if (!n) return "";
  if (RE_RECADO.test(n)) return "recado";
  if (RE_INDISPONIVEL.test(n)) return "indisponivel";
  if (RE_CAIXA_POSTAL.test(n)) return "caixa";
  if (RE_TRIAGEM.test(n)) return "triagem";
  if (RE_ESPERA.test(n)) return "espera";
  return "";
}
const MOTIVO_MAQUINA = {
  caixa: "Caixa postal",
  recado: "Recado da operadora (celular indisponível)",
  indisponivel: "Assistente de chamadas: pessoa indisponível",
};
// Pediu pra não ligar mais → contato vira "Não atender" no painel (nenhuma campanha liga de novo)
const RE_NAO_LIGAR = /\b(nao (me )?(liga|ligue|ligar|ligam|telefona) mais|para(r)? de (me )?ligar|nao quero (mais )?(receber )?(ligacao|ligacoes)|nao quero que (voces |vc )?(me )?liguem|(tira|tirar|remove|remover|apaga|apagar) (o )?(meu (numero|nome|telefone|contato)|eu) d(a|essa|esta) (sua )?lista|nao me ligue)\b/;
function pediuNaoLigar(texto) { return RE_NAO_LIGAR.test(normalizar(texto)); }
// Gente de verdade atendendo: "Alô?", "Oi", "Quem é?", "Pois não", "Boa tarde"
const RE_ALO_HUMANO = /^(e |mas )?(alo|oi|ola|opa|pronto|pois nao|sim|fala|diga|pode falar|quem (e|fala|ta falando|esta falando)|bom dia|boa tarde|boa noite|e ai|oi quem)\b/;
function ehAloHumano(texto) {
  const n = normalizar(texto);
  return !!n && n.split(" ").length <= 8 && RE_ALO_HUMANO.test(n);
}
// Espera em silêncio: sem "cortou aqui" e sem "você ainda está aí?" até o prazo
function entrarEmEspera(st, motivo, ms = ESPERA_MAX_MS) {
  const ate = Date.now() + ms;
  const novo = !st.esperaAte;
  if (ate > (st.esperaAte || 0)) st.esperaAte = ate;
  st.ultimaPergunta = ""; st.jaRepetiuPergunta = true;
  if (novo) L(st, `[espera] ⏳ ${motivo} — fico calado (até ${Math.round(ms / 1000)}s)`);
}

// ----- Nome -----
function escaparRegex(s) { return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
function reNome(nome) { return new RegExp(`(?<!\\p{L})${escaparRegex(nome)}(?!\\p{L})`, "giu"); }
function contarNome(texto, nome) {
  if (!nome) return 0;
  return (String(texto).match(reNome(nome)) || []).length;
}
function limparSobra(t) {
  t = t.replace(/\s+([.,!?])/g, "$1").replace(/,\s*,/g, ",").replace(/\s{2,}/g, " ").trim();
  t = t.replace(/^[,\s]+/, "");
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : t;
}
// Tira só o nome usado como chamamento ("Show, Ademar." / "Ademar, você…").
// No meio da frase ("O Ademar está aí?") o nome fica — senão sobra "O está aí?".
function tirarVocativos(texto, nome) {
  const n = escaparRegex(nome);
  return String(texto)
    .replace(new RegExp(`\\s*,\\s*(?<!\\p{L})${n}(?!\\p{L})(?=\\s*([,.!?…]|$))`, "giu"), "")
    .replace(new RegExp(`(^|[.!?…]\\s+)(?<!\\p{L})${n}(?!\\p{L})\\s*,\\s*`, "giu"), "$1");
}
function tirarNome(texto, nome) {
  if (!nome) return texto;
  return limparSobra(tirarVocativos(texto, nome));
}
function manterSoPrimeira(texto, nome) {
  const s = String(texto);
  const m = new RegExp(`(?<!\\p{L})${escaparRegex(nome)}(?!\\p{L})`, "iu").exec(s);
  if (!m) return s;
  const corte = m.index + m[0].length;
  return limparSobra(s.slice(0, corte) + tirarVocativos(s.slice(corte), nome));
}
// Erro de digitação do nome pelo Claude ("Adenar" no lugar de "Ademar")
function quaseIgual(a, b) {
  if (a === b) return true;
  if (Math.abs(a.length - b.length) > 1) return false;
  let i = 0, j = 0, dif = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) { i++; j++; continue; }
    if (++dif > 1) return false;
    if (a.length > b.length) i++;
    else if (b.length > a.length) j++;
    else { i++; j++; }
  }
  return dif + (a.length - i) + (b.length - j) <= 1;
}
function corrigirNome(texto, nome, protegidas) {
  if (!nome || nome.length < 5) return texto;
  const alvo = normalizar(nome);
  return String(texto).replace(/\p{Lu}\p{L}{3,}/gu, (w) => {
    const nw = normalizar(w);
    if (nw === alvo || protegidas?.has(nw)) return w;
    return quaseIgual(nw, alvo) ? nome : w;
  });
}

// ----- Perguntas -----
const PEDE_NUMERO = /\b(qual|me\s+(passa|diz|fala|informa|manda)|pode\s+(me\s+)?(passar|dizer|falar|informar|repetir)|repete|continua)\b[^?.!]{0,60}\b(n[uú]mero|whats|whatsapp|zap|telefone|celular|resto)\b/i;
function ehPergunta(texto) { return /\?["')\]]*\s*$/.test(String(texto)); }
// "Oi Frank, tudo bem?" não é pergunta do roteiro: não corta a fala nem vira re-pergunta
function ehPerguntaSocial(texto, nome) {
  let n = normalizar(nome ? tirarNome(texto, nome) : texto);
  n = n.replace(/^(oi|ola|opa|alo|e ai|bom dia|boa tarde|boa noite)\s*/, "").trim();
  if (/^(beleza|tudo certo)$/.test(n)) return true;
  // "tudo bem?", "Frank, tudo bem?", "como vai você?" — no máximo 5 palavras
  return n.split(" ").length <= 5 &&
    /(^| )(tudo (bem|bom|certo|joia|tranquilo|beleza|ok)|como (vai|voce esta|esta)|td bem)( com voce| ai)?$/.test(n);
}
function ultimaFrasePergunta(texto) {
  const frases = String(texto || "").match(/[^.!?…]+[.!?…]+/g) || [];
  for (let i = frases.length - 1; i >= 0; i--) {
    const f = frases[i].trim();
    if (ehPergunta(f)) return f;
  }
  return "";
}

// ----- Eco: o telefone da pessoa devolve a voz do Carlos (viva-voz) -----
function bigramas(p) { const out = []; for (let i = 0; i + 1 < p.length; i++) out.push(p[i] + " " + p[i + 1]); return out; }
function ehEcoProvavel(st, texto) {
  const p = normalizar(texto).split(" ").filter(Boolean);
  if (p.length < 4) return false;
  const ref = normalizar(juntarTexto(st.falaAnterior, st.falaAtual)).split(" ").filter(Boolean);
  if (ref.length < 4) return false;
  const conhecidos = new Set(bigramas(ref));
  const bg = bigramas(p);
  const iguais = bg.filter((b) => conhecidos.has(b)).length;
  return iguais / bg.length >= 0.85;
}

// ----- WhatsApp: número falado → dígitos (sem depender do Claude) -----
const NUM_UNIDADE = { zero: 0, um: 1, uma: 1, dois: 2, duas: 2, tres: 3, quatro: 4, cinco: 5, seis: 6, meia: 6, sete: 7, oito: 8, nove: 9 };
const NUM_DEZ = { dez: 10, onze: 11, doze: 12, treze: 13, quatorze: 14, catorze: 14, quinze: 15,
  dezesseis: 16, dezasseis: 16, dezessete: 17, dezassete: 17, dezoito: 18, dezenove: 19, dezanove: 19 };
const NUM_DEZENA = { vinte: 20, trinta: 30, quarenta: 40, cinquenta: 50, cincoenta: 50,
  sessenta: 60, setenta: 70, oitenta: 80, noventa: 90 };
const NUM_CENTENA = { cem: 100, cento: 100, duzentos: 200, trezentos: 300, quatrocentos: 400,
  quinhentos: 500, seiscentos: 600, setecentos: 700, oitocentos: 800, novecentos: 900 };
const DIGITO_FALADO = ["zero", "um", "dois", "três", "quatro", "cinco", "seis", "sete", "oito", "nove"];

// "dezoito nove nove sete quarenta cinquenta trinta" → "18997405030"
function palavrasParaDigitos(texto) {
  const toks = normalizar(texto).split(" ").filter(Boolean);
  let out = "", grupo = null, esperaE = false;
  const fecha = () => { if (grupo) out += String(grupo.valor); grupo = null; esperaE = false; };
  for (const t of toks) {
    if (/^\d+$/.test(t)) { fecha(); out += t; continue; }
    if (t === "e") { if (grupo && grupo.nivel >= 2) esperaE = true; continue; }
    let v = null, nivel = 0;
    if (Object.hasOwn(NUM_UNIDADE, t)) { v = NUM_UNIDADE[t]; nivel = 1; }
    else if (Object.hasOwn(NUM_DEZ, t)) { v = NUM_DEZ[t]; nivel = 1; }
    else if (Object.hasOwn(NUM_DEZENA, t)) { v = NUM_DEZENA[t]; nivel = 2; }
    else if (Object.hasOwn(NUM_CENTENA, t)) { v = NUM_CENTENA[t]; nivel = 3; }
    if (v === null) { fecha(); continue; }
    if (grupo && esperaE && nivel < grupo.nivel) {        // "quarenta e cinco", "cento e vinte"
      grupo.valor += v; grupo.nivel = nivel; esperaE = false;
      if (nivel === 1) fecha();
      continue;
    }
    fecha();
    if (nivel === 1) out += String(v);
    else grupo = { valor: v, nivel };
  }
  fecha();
  return out;
}
function dddValido(ddd) { return /^[1-9][1-9]$/.test(String(ddd || "")); }
function dddDoTelefone(tel) {
  let d = String(tel || "").replace(/\D/g, "");
  if (d.startsWith("55") && d.length >= 12) d = d.slice(2);
  const ddd = d.slice(0, 2);
  return dddValido(ddd) ? ddd : "";
}
// Aceita: DDD + 9 + 8 dígitos (celular) ou DDD + fixo. Sem DDD, usa o DDD da ligação.
function validarTelefone(digitos, dddPadrao = "") {
  let d = String(digitos || "").replace(/\D/g, "");
  if (!d) return "";
  if ((d.length === 12 || d.length === 13) && d.startsWith("55")) d = d.slice(2);
  if ((d.length === 11 || d.length === 12) && d.startsWith("0")) d = d.slice(1);
  if (d.length === 9 && d[0] === "9" && dddValido(dddPadrao)) d = dddPadrao + d;
  if (d.length === 11 && dddValido(d.slice(0, 2)) && d[2] === "9") return d;
  if (d.length === 10 && dddValido(d.slice(0, 2)) && /[2-5]/.test(d[2])) return d;
  return "";
}
function formatarTelefone(d) {
  if (!d) return "";
  if (d.length === 11) return `(${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7)}`;
  if (d.length === 10) return `(${d.slice(0, 2)}) ${d.slice(2, 6)}-${d.slice(6)}`;
  return d;
}
function dddPorExtenso(ddd) {
  const n = parseInt(ddd, 10);
  const dez = { 11: "onze", 12: "doze", 13: "treze", 14: "quatorze", 15: "quinze", 16: "dezesseis", 17: "dezessete", 18: "dezoito", 19: "dezenove" };
  if (dez[n]) return dez[n];
  const dezenas = { 2: "vinte", 3: "trinta", 4: "quarenta", 5: "cinquenta", 6: "sessenta", 7: "setenta", 8: "oitenta", 9: "noventa" };
  const dz = dezenas[Math.floor(n / 10)], un = n % 10;
  return un ? `${dz} e ${DIGITO_FALADO[un]}` : dz;
}
// "18997405030" → "dezoito, nove nove sete quatro zero, cinco zero três zero"
function lerTelefone(d) {
  const dig = (s) => s.split("").map((c) => DIGITO_FALADO[+c]).join(" ");
  const resto = d.slice(2);
  const corte = resto.length - 4;
  return `${dddPorExtenso(d.slice(0, 2))}, ${dig(resto.slice(0, corte))}, ${dig(resto.slice(corte))}`;
}
function classificarConfirmacao(texto) {
  const n = normalizar(texto);
  if (!n) return "";
  if (/\b(nao|errado|errada|incorreto|trocado|faltou|falta)\b/.test(n)) return "nao";
  if (n === "e" || /\b(sim|isso|certo|certinho|correto|exato|exatamente|perfeito|pode|beleza|ok|okay|uhum|aham|ta|confere|positivo|mesmo)\b/.test(n)) return "sim";
  return "";
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

function montarPersonaStreams(agente, contato, saudacao) {
  const base = (agente?.persona_prompt || "").trim() ||
    "Você é um atendente educado e prestativo de uma empresa.";
  const nome = primeiroNome(contato?.nome || "");
  const blocoNome = nome
    ? `O nome da pessoa é ${nome}. Veio do cadastro e é confiável. Use-o com naturalidade,
várias vezes se fizer sentido: ao concordar com ela, antes de um pedido, ao tratar
uma objeção. Nunca duas vezes na mesma resposta, e varie a posição — nem sempre no
começo da frase. Nome usado com naturalidade aproxima e ajuda a convencer.`
    : `Você NÃO sabe o nome desta pessoa. Não use nome nenhum e não pergunte o nome.`;

  const blocoFim = agente?.encerrar_automaticamente === false ? "" : `

COMO ENCERRAR A LIGAÇÃO:
Quando o objetivo estiver cumprido, ou a pessoa deixar claro que não tem interesse,
despeça-se em UMA frase curta e escreva [FIM] no final dela.
O [FIM] NÃO é falado: é o sinal para o sistema desligar.
Exemplo: "Fechado, te mando agora. Valeu e até mais! [FIM]"`;

  return `${base}

════════ REGRAS TÉCNICAS DESTA LIGAÇÃO ════════

VOCÊ JÁ FALOU ISTO ASSIM QUE A PESSOA ATENDEU:
"${saudacao}"
Não se apresente de novo, a não ser que a pessoa pergunte quem é.

SOBRE O NOME:
${blocoNome}
Nunca use um nome que você "ouviu" na ligação: a transcrição do telefone erra nomes.

SOBRE O QUE VOCÊ OUVE:
A transcrição do telefone às vezes erra e chega uma palavra sem sentido
("Presidente", "Paciente"). Se a resposta não fizer sentido, NÃO repita o que ouviu.

FORMATO DA FALA (isto vira áudio):
Números por extenso. Nada de listas, asteriscos ou emojis.
O único marcador que existe é [FIM]. Nunca escreva outra coisa entre colchetes.

SE PEDIREM PRA ESPERAR ("aguarde", "só um minutinho", "vou chamar ele"):
responda só "Tudo bem, eu aguardo." e pare. Não explique nada.

SE QUEM ATENDEU NÃO FOR A PESSOA (filho, esposa, secretária) ou disser que a conta
é de outra pessoa: pergunte se dá pra falar com ela agora. Se não der, pergunte o
melhor horário ou telefone pra falar com ela, agradeça e encerre. NÃO leia de volta
números que a pessoa ditar — o sistema anota. Diga só "Anotado, obrigado!".

════════ REGRAS DE OURO (valem mais que tudo acima) ════════
1. UMA pergunta por resposta. Fez a pergunta, PARE e espere a resposta.

2. A conversa já começou: NÃO cumprimente de novo (nada de "oi" ou "tudo bem?").

3. WHATSAPP:
   • Primeiro confirme se o número da ligação é o WhatsApp da pessoa.
   • Se FOR → despedida em uma frase.
   • Se NÃO for → peça o WhatsApp com o DDD, e deixe a pessoa falar com calma.
   • Quando a pessoa ditar o número, o SISTEMA lê de volta pra ela confirmar.
     Você NÃO repete o número. Se ela confirmou → despedida em uma frase.
   • Se mesmo assim não der certo, NÃO insista: diga que a equipe liga nesse
     mesmo número pra confirmar o WhatsApp, e encerre.

4. VALOR DA CONTA: pergunte no máximo duas vezes. Se não entender, diga
   "Sem problema, a simulação usa a foto da sua conta" e vá pro fechamento.
   O valor NÃO é obrigatório.

5. No máximo duas frases curtas por resposta. Quando o roteiro trouxer uma fala pronta
   (como a explicação de como funciona o desconto), diga ela inteira, do jeito que está.${blocoFim}`;
}

// ============================================================================
// A BOCA
// ============================================================================

async function sintetizar(texto, cfg, estado, { avisarLongo = true } = {}) {
  if (!ELEVENLABS_API_KEY || !cfg?.vozId) return null;
  const url = `${ELEVENLABS_URL}/v1/text-to-speech/${cfg.vozId}/stream?output_format=ulaw_8000`;
  const montar = (nivel) => {
    const s = cfg.settings;
    if (nivel === 0) {
      const v = { stability: s.stability, similarity_boost: s.similarity_boost, style: s.style };
      if (cfg.velocidade && cfg.velocidade !== 1.0) v.speed = cfg.velocidade;
      return v;
    }
    if (nivel === 1) return { stability: s.stability, similarity_boost: s.similarity_boost, style: s.style };
    return { stability: s.stability, similarity_boost: s.similarity_boost };
  };
  const pedir = (nivel) => fetch(url, {
    method: "POST",
    headers: { "xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ text: texto, model_id: ELEVENLABS_MODEL, language_code: "pt", voice_settings: montar(nivel) }),
    signal: AbortSignal.timeout(10000),
  });
  const inicio = Date.now();
  try {
    let resp = await pedir(estado.nivelVoz || 0);
    while (!resp.ok && (resp.status === 400 || resp.status === 422) && (estado.nivelVoz || 0) < 2) {
      estado.nivelVoz = (estado.nivelVoz || 0) + 1;
      LW(estado, `[boca] ajuste de voz recusado — tentando nível ${estado.nivelVoz}`);
      resp = await pedir(estado.nivelVoz);
    }
    if (!resp.ok) {
      const erro = await resp.text();
      LE(estado, `[boca] ElevenLabs recusou: ${resp.status} ${erro.slice(0, 300)}`);
      return null;
    }
    const audio = Buffer.from(await resp.arrayBuffer());
    const seg = audio.length / 8000;
    if (avisarLongo && seg > 6) LW(estado, `[boca] ⚠️ trecho longo: ${seg.toFixed(1)}s`);
    L(estado, `[boca] ${seg.toFixed(1)}s de áudio em ${Date.now() - inicio}ms`);
    return audio;
  } catch (e) {
    LE(estado, `[boca] erro: ${e?.message || e}`);
    return null;
  }
}

// Manda o áudio pra Twilio em levas e fecha com uma "marca" (t<turno>).
// A Twilio devolve a marca quando termina de TOCAR o áudio.
async function enviarAudio(ws, st, audio, meuTurno) {
  if (!audio || !st.streamSid) return false;
  if (st.turno !== meuTurno) { L(st, "[boca] interrompido antes de começar"); return false; }
  st.marcasPendentes++;
  // previsão de quando a Twilio termina de tocar tudo (μ-law 8kHz: 8 bytes = 1ms)
  st.fimPrevisto = Math.max(Date.now(), st.fimPrevisto || 0) + audio.length / 8;
  const PEDACO = 640, POR_LEVA = 16, ESPERA = 800;
  let desde = 0;
  for (let i = 0; i < audio.length; i += PEDACO) {
    if (st.turno !== meuTurno || ws.readyState !== 1) {
      // Se o turno mudou, o contador já foi zerado pelo turno novo: não mexe nele.
      if (st.turno === meuTurno) st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
      L(st, "[boca] 🛑 parei no meio da fala");
      return false;
    }
    ws.send(JSON.stringify({
      event: "media", streamSid: st.streamSid,
      media: { payload: audio.subarray(i, i + PEDACO).toString("base64") },
    }));
    if (++desde >= POR_LEVA) { desde = 0; await pausa(ESPERA); }
  }
  if (st.turno !== meuTurno || ws.readyState !== 1) {
    if (st.turno === meuTurno) st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
    return false;
  }
  ws.send(JSON.stringify({ event: "mark", streamSid: st.streamSid, mark: { name: `t${meuTurno}` } }));
  return true;
}

// Marcadores entre colchetes nunca viram voz ("[AGUARDANDO]" foi falado no piloto)
function semMarcadores(texto) {
  return String(texto || "").replace(/\[[^\]]{1,30}\]/g, " ").replace(/\s{2,}/g, " ").trim();
}
async function falarComMinhaVoz(ws, st, texto, meuTurno) {
  if (st.fechado || st.turno !== meuTurno) return false;   // ligação caiu ou turno velho: não gasta voz
  texto = semMarcadores(texto);
  if (!texto) return false;
  const cfg = { vozId: st.vozId, velocidade: st.velocidade, settings: st.vozSettings };
  // a saudação repetida (depois do assistente de chamadas) sai do cache, na hora
  const audio = texto === st.saudacao ? await obterSaudacao(cfg, texto, st).promessa : await sintetizar(texto, cfg, st);
  if (!audio) return false;
  return enviarAudio(ws, st, audio, meuTurno);
}

const cacheSaudacao = new Map();
const CACHE_SAUDACAO_MAX = 60;
function chaveSaudacao(cfg, texto) {
  const s = cfg.settings;
  return [cfg.vozId, ELEVENLABS_MODEL, cfg.velocidade, s.stability, s.similarity_boost, s.style, texto].join("|");
}
function obterSaudacao(cfg, texto, estado) {
  const chave = chaveSaudacao(cfg, texto);
  if (cacheSaudacao.has(chave)) return { promessa: cacheSaudacao.get(chave), doCache: true };
  if (cacheSaudacao.size >= CACHE_SAUDACAO_MAX) cacheSaudacao.delete(cacheSaudacao.keys().next().value);
  const promessa = sintetizar(texto, cfg, estado, { avisarLongo: false }).then((buf) => {
    if (!buf) cacheSaudacao.delete(chave);
    return buf;
  });
  cacheSaudacao.set(chave, promessa);
  return { promessa, doCache: false };
}

// ============================================================================
// FILA
// ============================================================================

const ROTULO_STATUS = {
  "busy": "Ocupado", "no-answer": "Não atendeu", "failed": "Falha na chamada",
  "canceled": "Cancelada", "completed": "Desligou antes de atender",
};

async function resolverNaoAtendida(supabase, lig, motivo) {
  let resolvi = false;
  try {
    const { data: atual } = await supabase.from("ligacoes").select("status").eq("id", lig.id).maybeSingle();
    if (!atual || atual.status !== "ligando") return;
    await supabase.from("ligacoes").update({
      status: "sem_resposta", resultado: motivo, finalizada_em: new Date().toISOString(),
    }).eq("id", lig.id).eq("status", "ligando");
    resolvi = true;
    if (!lig.campanha_id || !lig.contato_id) return;
    const { data: camp } = await supabase.from("campanhas")
      .select("max_tentativas").eq("id", lig.campanha_id).maybeSingle();
    const maxTent = camp?.max_tentativas ?? 2;
    const { data: cc } = await supabase.from("campanha_contatos")
      .select("id, tentativas, status")
      .eq("campanha_id", lig.campanha_id).eq("contato_id", lig.contato_id).maybeSingle();
    if (!cc || cc.status !== "ligando") return;
    const tent = cc.tentativas || 0;
    const volta = tent < maxTent;
    await supabase.from("campanha_contatos").update({
      status: volta ? "na_fila" : "sem_resposta", atualizado_em: new Date().toISOString(),
    }).eq("id", cc.id).eq("status", "ligando");
    console.log(`[fila] ${motivo} — tentativa ${tent}/${maxTent} → ` +
      (volta ? "🔄 VOLTOU PRA FILA" : "❌ esgotou as tentativas"));
  } catch (e) {
    console.error("[fila] erro ao resolver:", e?.message);
  } finally {
    // uma linha liberou: chama o próximo da campanha
    if (resolvi && lig.campanha_id) continuarDepois(lig.campanha_id);
  }
}

// ============================================================================
// DISCADOR — a campanha anda sozinha até o fim
// ============================================================================
// Quem chama discar():
//   • o botão Iniciar do painel (POST /campanhas/iniciar)
//   • o fim de cada ligação (continuarDepois)
//   • o relógio, a cada CAMPANHA_CHECA_S (retentativas, agendadas, segurança)
// Regras: no máximo MAX_CONCURRENT_CALLS no ar por campanha; nunca liga de novo
// pra quem já conversou nesta campanha; tentativas contadas pelo histórico real
// de ligações (o Iniciar do painel zera o status e as tentativas da fila).

function agoraEmSP() {
  const partes = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Sao_Paulo", weekday: "short", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
  }).formatToParts(new Date());
  const v = (t) => partes.find((p) => p.type === t)?.value || "";
  const dia = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 }[v("weekday")];
  return { dia, hora: (parseInt(v("hour"), 10) || 0) + (parseInt(v("minute"), 10) || 0) / 60 };
}
function dentroDoHorario() {
  const { dia, hora } = agoraEmSP();
  if (dia === 0) return LIGAR_DOMINGO && hora >= HORA_INICIO && hora < HORA_FIM_SABADO;
  return hora >= HORA_INICIO && hora < (dia === 6 ? HORA_FIM_SABADO : HORA_FIM);
}
function fmtHora(h) {
  return `${String(Math.floor(h)).padStart(2, "0")}:${String(Math.round((h % 1) * 60)).padStart(2, "0")}`;
}
function descricaoHorario() {
  return `seg–sex ${fmtHora(HORA_INICIO)}–${fmtHora(HORA_FIM)}, sáb ${fmtHora(HORA_INICIO)}–${fmtHora(HORA_FIM_SABADO)}, ` +
    (LIGAR_DOMINGO ? `dom ${fmtHora(HORA_INICIO)}–${fmtHora(HORA_FIM_SABADO)}` : "dom não");
}

// Uma rodada de discagem por vez em cada campanha (evita discar o mesmo contato 2x)
const rodadaDaCampanha = new Map();
function umaPorVez(campanhaId, tarefa) {
  const antes = rodadaDaCampanha.get(campanhaId) || Promise.resolve();
  const agora = antes.then(tarefa, tarefa);
  const guarda = agora.catch(() => {});
  rodadaDaCampanha.set(campanhaId, guarda);
  guarda.then(() => { if (rodadaDaCampanha.get(campanhaId) === guarda) rodadaDaCampanha.delete(campanhaId); });
  return agora;
}

async function historicoDaCampanha(supabase, campanhaId) {
  const todas = [];
  for (let de = 0; de < 50000; de += 1000) {
    const { data, error } = await supabase.from("ligacoes")
      .select("contato_id, status, iniciada_em").eq("campanha_id", campanhaId)
      .order("iniciada_em", { ascending: true }).range(de, de + 999);
    if (error) throw new Error("histórico de ligações: " + error.message);
    todas.push(...(data || []));
    if (!data || data.length < 1000) break;
  }
  return todas;
}

const avisoForaDoHorario = new Map();

// Erros da Twilio que são da CONTA, não do número do contato
// 20003 login · 20005 conta inativa · 20008 credencial de teste · 21210/21212 número de origem
// 21215 permissão de país · 21219 conta de teste (só liga pra número verificado) · 10001/10002 conta
const ERROS_DA_CONTA_TWILIO = new Set([20003, 20005, 20008, 21210, 21212, 21215, 21219, 10001, 10002]);
function erroDaContaTwilio(http, codigo) {
  return http === 401 || http === 403 || http === 404 || ERROS_DA_CONTA_TWILIO.has(Number(codigo));
}

async function discar(campanhaId, origem = "painel") {
  return umaPorVez(campanhaId, async () => {
    if (!twilioClient) return { status: 500, corpo: { error: "Twilio não configurado" } };
    const sb = await getSupabaseLogado();
    if (!sb) return { status: 500, corpo: { error: "login Supabase falhou" } };
    const supabase = sb.client, userId = sb.userId;
    const manual = origem === "painel";
    const foraDoHorario = !dentroDoHorario();

    const campanha = await carregarCampanha(supabase, campanhaId);
    if (!campanha) return { status: 404, corpo: { error: "campanha não encontrada" } };
    const nomeCamp = campanha.nome || campanhaId.slice(0, 8);
    if (!manual && campanha.status !== "em_andamento") {
      return { status: 200, corpo: { started: false, dialed: 0, message: `Campanha ${campanha.status}.` } };
    }
    // Campanha de TESTE = até 3 contatos (ex.: só o seu número). Funciona como antes:
    // Iniciar liga na hora pra todos dela (a qualquer hora) e ela não se encerra sozinha,
    // então dá pra testar de novo com Pausar → Iniciar.
    const { count: totalContatos } = await supabase.from("campanha_contatos")
      .select("id", { count: "exact", head: true }).eq("campanha_id", campanhaId);
    const teste = totalContatos != null && totalContatos <= MAX_CONCURRENT_CALLS;

    if (!manual && foraDoHorario) {
      // Fila vazia: conclui mesmo fora do horário (campanha de verdade)
      const { count: pendentes, error: erroPend } = await supabase.from("campanha_contatos")
        .select("id", { count: "exact", head: true })
        .eq("campanha_id", campanhaId).in("status", ["na_fila", "ligando"]);
      if (!erroPend && pendentes === 0) {
        if (teste) return { status: 200, corpo: { started: false, dialed: 0, message: "Campanha de teste: nada pra ligar." } };
        await supabase.from("campanhas").update({ status: "concluida" }).eq("id", campanhaId);
        console.log(`[discador] 🏁 "${nomeCamp}" concluída — ninguém mais pra ligar`);
        return { status: 200, corpo: { started: true, dialed: 0, message: "Campanha concluída." } };
      }
      if (Date.now() - (avisoForaDoHorario.get(campanhaId) || 0) > 3600000) {
        avisoForaDoHorario.set(campanhaId, Date.now());
        console.log(`[discador] ⏸️ "${nomeCamp}": fora do horário (${descricaoHorario()}) — continuo quando abrir`);
      }
      return { status: 200, corpo: { started: false, dialed: 0, message: "Fora do horário de ligação." } };
    }
    // Iniciar clicado fora do horário numa campanha de verdade: ninguém é chamado agora;
    // ela fica "em andamento" e começa sozinha quando o horário abrir.
    if (manual && foraDoHorario && !teste) {
      await supabase.from("campanhas").update({ status: "em_andamento" }).eq("id", campanhaId);
      avisoForaDoHorario.set(campanhaId, Date.now());
      console.log(`[discador] ⏸️ "${nomeCamp}": Iniciar fora do horário — ninguém foi chamado agora. ` +
        `A campanha começa sozinha quando o horário abrir (${descricaoHorario()})`);
      return { status: 200, corpo: { started: true, dialed: 0, message: "Fora do horário: começa sozinha quando abrir." } };
    }
    // Iniciar numa campanha de teste: liga de novo pra todos dela, mesmo quem já conversou
    const religar = manual && teste;
    const agente = await carregarAgente(supabase, campanha.agente_id);
    const maxTent = campanha.max_tentativas ?? 2;

    // Histórico real de cada contato nesta campanha
    const hist = await historicoDaCampanha(supabase, campanhaId);
    const porContato = new Map();
    const limiteNoAr = Date.now() - VARRER_MINUTOS * 60 * 1000;
    let noAr = 0;
    for (const l of hist) {
      if (!l.contato_id) continue;
      const h = porContato.get(l.contato_id) || { tentativas: 0, conversou: false, noAr: false, ultima: 0 };
      h.tentativas++;
      const t = l.iniciada_em ? Date.parse(l.iniciada_em) : 0;
      if (t > h.ultima) h.ultima = t;
      if (l.status === "atendida") h.conversou = true;
      if (l.status === "ligando" && t > limiteNoAr && !h.noAr) { h.noAr = true; noAr++; }
      porContato.set(l.contato_id, h);
    }
    const vagas = Math.max(0, MAX_CONCURRENT_CALLS - noAr);

    const { data: fila, error: erroFila } = await supabase.from("campanha_contatos")
      .select("id, contato_id, status, tentativas, atualizado_em")
      .eq("campanha_id", campanhaId).eq("status", "na_fila")
      .order("tentativas", { ascending: true }).order("atualizado_em", { ascending: true })
      .limit(500);
    if (erroFila) throw new Error("fila da campanha: " + erroFila.message);

    const marcar = (id, status) => supabase.from("campanha_contatos")
      .update({ status, atualizado_em: new Date().toISOString() }).eq("id", id);
    const escolhidos = [];
    let esperando = 0;
    for (const item of fila || []) {
      const h = porContato.get(item.contato_id);
      if (h?.noAr) continue;                                                   // está no telefone agora
      if (!religar) {
        if (h?.conversou) { await marcar(item.id, "concluida"); continue; }     // já falou com o Carlos
        if (h && h.tentativas >= maxTent) { await marcar(item.id, "sem_resposta"); continue; }
        if (h && h.tentativas > 0 && Date.now() - h.ultima < RETENTAR_APOS_MIN * 60000) { esperando++; continue; }
      }
      if (escolhidos.length < vagas) escolhidos.push({ item, h });
      else esperando++;
    }

    const host = PUBLIC_HOST;
    if (!host) return { status: 500, corpo: { error: "PUBLIC_HOST não configurado" } };
    const enc = encodeURIComponent;
    let dialed = 0, erroConta = "";
    for (const { item, h } of escolhidos) {
      const contato = await carregarContato(supabase, item.contato_id);
      if (!contato?.telefone) {
        await marcar(item.id, "falhou");
        console.log(`[discador] ⚠️ contato ${item.contato_id} sem telefone — pulei`);
        continue;
      }
      if (contato.status === "nao_atender") {
        await marcar(item.id, "falhou");
        console.log(`[discador] 🚫 ${contato.nome || contato.telefone} está como "Não atender" — pulei`);
        continue;
      }
      const tentativa = (h?.tentativas || 0) + 1;

      let twimlUrl;
      if (MOTOR_PADRAO === "streams") {
        twimlUrl =
          `https://${host}/twiml-streams?campanha_id=${enc(campanhaId)}` +
          `&contato_id=${enc(item.contato_id)}&agente_id=${enc(campanha.agente_id || "")}`;
        // gera a saudação (com o nome) enquanto o telefone toca
        if (agente) obterSaudacao(configVoz(agente), textoSaudacao(agente, contato), { nivelVoz: 0 });
      } else {
        const saudacao = textoSaudacao(agente, contato);
        twimlUrl =
          `https://${host}/twiml?campanha_id=${enc(campanhaId)}&contato_id=${enc(item.contato_id)}` +
          `&saudacao=${enc(saudacao)}&voice=${enc(ELEVENLABS_VOICE_ID)}` +
          `&language=${enc(agente?.idioma || "pt-BR")}`;
      }

      try {
        const call = await twilioClient.calls.create(opcoesDaChamada(contato.telefone, twimlUrl));
        await supabase.from("ligacoes").insert({
          user_id: userId, campanha_id: campanhaId, contato_id: item.contato_id,
          status: "ligando", twilio_call_sid: call.sid, iniciada_em: new Date().toISOString(),
        });
        await supabase.from("campanha_contatos").update({
          status: "ligando", tentativas: tentativa, atualizado_em: new Date().toISOString(),
        }).eq("id", item.id);
        dialed++;
        console.log(`[discador] ☎️ ligando para ${contato.nome || contato.telefone} ` +
          (religar ? "(campanha de teste)" : `(tentativa ${tentativa}/${maxTent})`) + ` → [${call.sid.slice(-4)}]`);
      } catch (err) {
        const cod = err?.status || err?.statusCode || 0;
        const passageiro = !cod || cod === 429 || cod >= 500;
        if (!passageiro && erroDaContaTwilio(cod, err?.code)) {
          // problema da CONTA (login, número de origem, conta de teste, permissão de país):
          // pausa a campanha em vez de marcar a lista inteira como "falhou"
          erroConta = `HTTP ${cod}, código ${err?.code || "?"}: ${err?.message}`;
          await supabase.from("campanhas").update({ status: "pausada" }).eq("id", campanhaId);
          console.error(`[discador] 🛑 "${nomeCamp}" PAUSADA — a Twilio recusou por um problema da CONTA (${erroConta}). ` +
            "Nenhum contato foi marcado como falhou. Corrija na Twilio e clique Iniciar de novo.");
          break;
        }
        console.error(`[discador] ❌ erro ao ligar para ${contato.nome || contato.telefone}: ${err?.message}` +
          (passageiro ? " — tento de novo depois" : ""));
        if (passageiro) esperando++;
        else await marcar(item.id, "falhou");
      }
    }

    if (erroConta) {
      // 502 no Iniciar: o painel mostra que não iniciou (e não troca pra "em andamento")
      return manual
        ? { status: 502, corpo: { error: `Twilio recusou (problema da conta): ${erroConta}` } }
        : { status: 200, corpo: { started: false, dialed, message: "Campanha pausada: erro da conta Twilio." } };
    }

    if (dialed === 0 && esperando === 0 && noAr === 0 && !teste) {
      await supabase.from("campanhas").update({ status: "concluida" }).eq("id", campanhaId);
      console.log(`[discador] 🏁 "${nomeCamp}" concluída — ninguém mais pra ligar`);
      return { status: 200, corpo: { started: true, dialed: 0, message: "Campanha concluída." } };
    }
    if (manual) await supabase.from("campanhas").update({ status: "em_andamento" }).eq("id", campanhaId);
    if (dialed > 0 || manual) {
      console.log(`[discador] "${nomeCamp}" (${origem}): ${dialed} discada(s) | no ar: ${noAr + dialed} ` +
        `| esperando: ${esperando}` +
        (religar ? ` | 🧪 campanha de teste (até ${MAX_CONCURRENT_CALLS} contatos)` +
          (foraDoHorario ? ": liguei mesmo fora do horário" : "") : ""));
    }
    return { status: 200, corpo: { started: true, dialed, no_ar: noAr + dialed, na_fila: esperando, motor: MOTOR_PADRAO } };
  });
}

// Uma ligação terminou: depois de uma pausinha, chama o próximo da campanha
function continuarDepois(campanhaId, ms = PAUSA_ENTRE_LIGACOES_MS) {
  if (!campanhaId) return;
  setTimeout(() => {
    discar(campanhaId, "próxima").catch((e) => console.error("[discador] erro ao continuar:", e?.message || e));
  }, ms);
}

// Relógio: retentativas no tempo certo, campanhas agendadas e rede de segurança
setInterval(async () => {
  try {
    const sb = await getSupabaseLogado();
    if (!sb || !twilioClient) return;
    const { data: camps, error } = await sb.client.from("campanhas")
      .select("id, nome, status, agendada_para").in("status", ["em_andamento", "agendada"]);
    if (error) { console.error("[discador] relógio:", error.message); return; }
    for (const c of camps || []) {
      if (c.status === "agendada") {
        if (!c.agendada_para || Date.parse(c.agendada_para) > Date.now() || !dentroDoHorario()) continue;
        await sb.client.from("campanhas").update({ status: "em_andamento" }).eq("id", c.id).eq("status", "agendada");
        console.log(`[discador] ⏰ "${c.nome || c.id}" começou no horário agendado`);
      }
      await discar(c.id, "relógio");
    }
  } catch (e) {
    console.error("[discador] relógio:", e?.message || e);
  }
}, CAMPANHA_CHECA_S * 1000);

// ----------------------- App HTTP -----------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/", (req, res) => res.send("VozIA — motor de voz online ✅"));

app.post("/twilio/status", async (req, res) => {
  res.status(204).end();
  if (!VOICE_BACKEND_SECRET || (req.query.k || "") !== VOICE_BACKEND_SECRET) {
    console.warn("[twilio-status] chamada sem chave válida — ignorada");
    return;
  }
  const sid = req.body?.CallSid;
  const status = String(req.body?.CallStatus || "").toLowerCase();
  const duracao = parseInt(req.body?.CallDuration || "0", 10);
  if (!sid) return;
  // detalhes que a Twilio manda em falha/ocupado (ajudam a entender "Falha na chamada")
  const detalhes = ["SipResponseCode", "ErrorCode", "ErrorMessage", "AnsweredBy"]
    .filter((k) => req.body?.[k]).map((k) => `${k}=${req.body[k]}`).join(" ");
  console.log(`[twilio-status] ${sid} → ${status} (${duracao}s)${detalhes ? " | " + detalhes : ""}`);
  if (status === "completed" && duracao > 0) {
    console.log(`[twilio-status] atendida (${duracao}s) — o motor cuida do registro`);
    return;
  }
  await pausa(MARGEM_CALLBACK_MS);
  try {
    const sb = await getSupabaseLogado();
    if (!sb) return;
    const { data: lig } = await sb.client.from("ligacoes")
      .select("id, status, campanha_id, contato_id").eq("twilio_call_sid", sid).maybeSingle();
    if (!lig || lig.status !== "ligando") return;
    await resolverNaoAtendida(sb.client, lig, ROTULO_STATUS[status] || "Sem conversa");
  } catch (e) {
    console.error("[twilio-status] erro:", e?.message);
  }
});

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

function opcoesDaChamada(telefone, twimlUrl) {
  const o = { to: telefone, from: TWILIO_FROM, url: twimlUrl };
  if (DETECTAR_SECRETARIA) o.machineDetection = "Enable";
  if (GRAVAR_LIGACOES) { o.record = true; o.recordingChannels = "dual"; }
  if (PUBLIC_HOST && VOICE_BACKEND_SECRET) {
    o.statusCallback = `https://${PUBLIC_HOST}/twilio/status?k=${encodeURIComponent(VOICE_BACKEND_SECRET)}`;
    o.statusCallbackMethod = "POST";
    o.statusCallbackEvent = ["completed"];
  }
  return o;
}

app.post("/campanhas/iniciar", async (req, res) => {
  const auth = req.headers.authorization || "";
  if (!VOICE_BACKEND_SECRET || auth !== `Bearer ${VOICE_BACKEND_SECRET}`) {
    return res.status(401).json({ error: "não autorizado" });
  }
  const campanhaId = req.body?.campanha_id;
  if (!campanhaId) return res.status(400).json({ error: "campanha_id é obrigatório" });
  try {
    // Disca as primeiras; depois a campanha anda sozinha (continuarDepois + relógio)
    const r = await discar(campanhaId, "painel");
    res.status(r.status).json(r.corpo);
  } catch (e) {
    console.error("[/campanhas/iniciar] erro:", e?.message || e);
    res.status(500).json({ error: "erro ao iniciar campanha" });
  }
});

app.get("/streams/teste", async (req, res) => {
  if (!PERMITIR_TESTE) {
    console.warn("[streams/teste] acesso negado — rota fechada");
    return res.status(404).json({ error: "não encontrado" });
  }
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
    const opcoes = {
      to: para, from: TWILIO_FROM,
      url: `https://${host}/twiml-streams?agente_id=${enc(agenteId)}&contato_id=${enc(contatoId)}`,
    };
    if (GRAVAR_LIGACOES) { opcoes.record = true; opcoes.recordingChannels = "dual"; }
    const call = await twilioClient.calls.create(opcoes);
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

setInterval(async () => {
  try {
    const sb = await getSupabaseLogado();
    if (!sb) return;
    const limite = new Date(Date.now() - VARRER_MINUTOS * 60 * 1000).toISOString();
    const { data: presas } = await sb.client.from("ligacoes")
      .select("id, status, campanha_id, contato_id, twilio_call_sid")
      .eq("status", "ligando").lt("iniciada_em", limite).limit(50);
    if (!presas || presas.length === 0) return;
    console.log(`[varredura] 🧹 ${presas.length} ligação(ões) presa(s) há mais de ${VARRER_MINUTOS}min`);
    for (const p of presas) await resolverNaoAtendida(sb.client, p, "Sem retorno da operadora");
  } catch (e) {
    console.error("[varredura] erro:", e?.message);
  }
}, 120000);

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
          if (session.agente?.saudacao_inicial) saudacao = textoSaudacao(session.agente, session.contato);
        } catch (e) { console.error("[setup] erro:", e?.message); }
      }
      session.saudacao = saudacao;
      session.systemPrompt = montarPersonaStreams(session.agente, session.contato, saudacao);
      session.transcript.push(`Assistente: ${saudacao}`);
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
          try { ws.send(JSON.stringify({ type: "text", token: d.replace(/\[FIM\]/g, ""), last: false })); } catch {}
        });
        await stream.finalMessage();
        ws.send(JSON.stringify({ type: "text", token: "", last: true }));
        session.currentStream = null;
        if (texto.trim()) {
          session.history.push({ role: "assistant", content: texto });
          session.transcript.push(`Assistente: ${texto}`);
        }
      } catch (e) {
        if (!ehAbortado(e)) console.error("[prompt] erro Claude:", e?.message);
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

async function gravarLigacao({ supabase, callSid, campanhaId, contatoId, transcricao, houveConversa, duracao, extras = {}, st = null }) {
  if (!supabase || !callSid) {
    L(st, "[relatorio] ligação de teste — nada a gravar no banco");
    return;
  }
  const zapOk = extras.whatsappConfirmado ? formatarTelefone(extras.whatsappConfirmado) : "";
  const zapDuvida = !zapOk && extras.whatsappNaoConfirmado ? formatarTelefone(extras.whatsappNaoConfirmado) : "";
  const fatos = [];
  if (zapOk) fatos.push(`O sistema anotou o WhatsApp ${zapOk} e a pessoa CONFIRMOU. Use exatamente este número no "resultado".`);
  if (zapDuvida) fatos.push(`O sistema entendeu o WhatsApp ${zapDuvida}, mas a pessoa NÃO confirmou. Cite-o como "não confirmado".`);
  // Números ditos por extenso ("três meia cinco dois…"): o motor converte, o resumo não chuta
  for (const linha of String(transcricao || "").split("\n")) {
    if (!linha.startsWith("Cliente:")) continue;
    const dig = palavrasParaDigitos(linha.slice(8));
    if (dig.length >= 8 && !fatos.some((f) => f.includes(dig))) {
      fatos.push(`A pessoa ditou os dígitos ${dig} (convertidos pelo sistema; "meia" = 6). Se citar esse número, use exatamente esses dígitos.`);
    }
  }

  let resultado = null, sentimento = null, nota = null;
  if (anthropic && houveConversa && transcricao.trim()) {
    try {
      const r = await anthropic.messages.create({
        model: CLAUDE_MODEL, max_tokens: 280,
        messages: [{ role: "user", content:
          `Abaixo está a transcrição de uma ligação de prospecção. Responda APENAS com um JSON válido, sem texto extra e sem markdown, no formato exato: {"resultado":"resumo curto do que aconteceu e qual o próximo passo","sentimento":"positivo|neutro|negativo","nota": número de 1 a 10 avaliando o quanto este contato é promissor, ou null}.
Se a pessoa informou um número de WhatsApp diferente do número da ligação, inclua esse número no "resultado" em algarismos, no formato (DD) 9XXXX-XXXX. Se o número ficou incompleto ou confuso, diga isso claramente.
${fatos.length ? "\nFatos confirmados pelo sistema (valem mais que a transcrição):\n" + fatos.join("\n") + "\n" : ""}
Transcrição:\n\n${transcricao}` }],
      });
      let txt = (r.content?.[0]?.text || "").trim().replace(/^```(json)?/i, "").replace(/```$/, "").trim();
      const obj = JSON.parse(txt);
      resultado = obj.resultado ?? null;
      sentimento = ["positivo", "neutro", "negativo"].includes(obj.sentimento) ? obj.sentimento : null;
      nota = typeof obj.nota === "number" ? obj.nota : null;
    } catch (e) { LE(st, `[relatorio] erro ao resumir: ${e?.message}`); }
  }
  // O número confirmado entra no resultado mesmo se o resumo falhar ou esquecer
  if (zapOk && !String(resultado || "").includes(zapOk)) resultado = juntarTexto(resultado, `WhatsApp confirmado: ${zapOk}.`);
  if (zapDuvida && !String(resultado || "").includes(zapDuvida)) resultado = juntarTexto(resultado, `WhatsApp NÃO confirmado: ${zapDuvida}.`);
  if (!houveConversa && extras.motivoSemConversa && !resultado) resultado = extras.motivoSemConversa;
  if (extras.naoLigarMais) resultado = juntarTexto(resultado, "PEDIU PARA NÃO LIGAR MAIS (contato marcado como Não atender).");
  if (resultado || sentimento || nota !== null) L(st, `[relatorio] resumo: ${sentimento} | nota ${nota} | ${resultado}`);

  try {
    await supabase.from("ligacoes").update({
      status: houveConversa ? "atendida" : "sem_resposta",
      duracao_segundos: duracao, transcricao, resultado, sentimento, nota,
      finalizada_em: new Date().toISOString(),
    }).eq("twilio_call_sid", callSid);

    if (contatoId && extras.naoLigarMais) {
      await supabase.from("contatos").update({ status: "nao_atender" }).eq("id", contatoId);
      L(st, "[contato] 🚫 marcado como \"Não atender\" — nenhuma campanha liga de novo");
    }
    if (campanhaId && contatoId) {
      if (houveConversa) {
        await supabase.from("campanha_contatos").update({
          status: "concluida", atualizado_em: new Date().toISOString(),
        }).eq("campanha_id", campanhaId).eq("contato_id", contatoId);
        L(st, "[fila] ✅ conversa registrada — contato concluído");
      } else {
        const { data: camp } = await supabase.from("campanhas")
          .select("max_tentativas").eq("id", campanhaId).maybeSingle();
        const maxTent = camp?.max_tentativas ?? 2;
        const { data: cc } = await supabase.from("campanha_contatos")
          .select("id, tentativas").eq("campanha_id", campanhaId).eq("contato_id", contatoId).maybeSingle();
        const tent = cc?.tentativas || 0;
        const volta = tent < maxTent;
        if (cc) {
          await supabase.from("campanha_contatos").update({
            status: volta ? "na_fila" : "sem_resposta", atualizado_em: new Date().toISOString(),
          }).eq("id", cc.id);
          L(st, `[fila] ${extras.motivoSemConversa ? extras.motivoSemConversa.toLowerCase() : "atendeu mas não falou"} — tentativa ${tent}/${maxTent} → ` +
            (volta ? "🔄 VOLTOU PRA FILA" : "❌ esgotou as tentativas"));
        }
      }
    }
    L(st, `[relatorio] ✅ ligação gravada: ${callSid} (${duracao}s)`);
  } catch (e) { LE(st, `[relatorio] erro ao gravar: ${e?.message}`); }
  // a linha liberou: chama o próximo da campanha
  if (campanhaId) continuarDepois(campanhaId);
}

// ===================== MOTOR NOVO (Media Streams) =====================

function aquecerCerebro(st) {
  if (!anthropic || !st.persona) return;
  const t0 = Date.now();
  anthropic.messages
    .create({ model: CLAUDE_MODEL, max_tokens: 1, system: st.persona, messages: [{ role: "user", content: "oi" }] })
    .then(() => L(st, `[cerebro] 🔥 conexão aquecida em ${Date.now() - t0}ms`))
    .catch((e) => LW(st, `[cerebro] aquecimento falhou: ${e?.message}`));
}

// ----- Turnos -----
// Todo turno do Carlos (resposta do Claude, frase do sistema, resgate) começa aqui.
// Fala do Carlos cortada no meio (pessoa interrompeu): entra no histórico e na transcrição,
// senão o Claude não sabe o que já disse e a transcrição parece que ele ficou mudo.
function registrarParcial(st) {
  const p = st.parcial;
  st.parcial = null;
  if (!p || !p.texto) return;
  st.historico.push({ role: "assistant", content: `${p.texto} —` });
  st.transcricao.push(`Agente: ${p.texto} (interrompido)`);
}
function iniciarTurno(st) {
  registrarParcial(st);
  st.turno++;
  st.falando = true; st.marcasPendentes = 0;
  st.turnoPronto = false; st.turnoEmPergunta = false;
  st.respostaNaFila = ""; st.fimPendente = false;
  st.podeInterromperApos = Date.now() + 800;
  if (st.falaAtual) st.falaAnterior = st.falaAtual;
  st.falaAtual = "";
  return st.turno;
}
// Todo o áudio do turno já foi mandado pra Twilio. Se nada ficou tocando, já libera o ouvido.
function turnoEnviado(st, meu) {
  if (st.turno !== meu) return;
  st.turnoPronto = true;
  if (st.marcasPendentes === 0 && st.falando) st.fimDaFala?.();
}

function calarABoca(ws, st, motivo) {
  st.turno++;
  st.marcasPendentes = 0; st.falando = false; st.claudePensando = false;
  st.turnoPronto = false; st.turnoEmPergunta = false;
  st.respostaNaFila = ""; st.fimPendente = false; st.ultimaPergunta = "";
  if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} st.streamClaude = null; }
  if (st.streamSid && ws.readyState === 1) {
    try { ws.send(JSON.stringify({ event: "clear", streamSid: st.streamSid })); } catch {}
  }
  st.calado_desde = Date.now();
  st.fimPrevisto = Date.now();
  L(st, `[barge-in] ✋ CALEI A BOCA — ${motivo}`);
}

async function desligar(st, motivo) {
  if (st.encerrando) return;
  st.encerrando = true;
  L(st, `[fim] desligando — ${motivo}`);
  if (twilioClient && st.callSid) {
    try {
      await twilioClient.calls(st.callSid).update({ status: "completed" });
      L(st, "[fim] ligação encerrada pelo motor ✅");
    } catch (e) { LE(st, `[fim] erro ao desligar: ${e?.message}`); }
  }
}

// Frase avulsa do vigia (re-pergunta, resgate, despedida por silêncio)
async function falarAvulso(ws, st, texto, tipo = "resgate", { encerrar = false } = {}) {
  const meu = iniciarTurno(st);
  if (encerrar) st.fimPendente = true;
  st.historico.push({ role: "assistant", content: texto });
  st.transcricao.push(`Agente: ${texto}`);
  st.falaAtual = texto;
  st.turnoEmPergunta = ehPergunta(texto);
  L(st, `[${tipo}] "${texto}"`);
  await falarComMinhaVoz(ws, st, texto, meu);
  turnoEnviado(st, meu);
  return meu;
}

// Frase fixa do sistema respondendo a uma fala da pessoa (confirmação do número)
async function falarFixo(ws, st, falaDoCliente, texto, { encerrar = false } = {}) {
  const meu = iniciarTurno(st);
  st.tentativasResgate = 0; st.nomeNoTurno = 0;
  st.ultimaPergunta = ""; st.jaRepetiuPergunta = false;
  if (encerrar && st.encerrarAuto) st.fimPendente = true;
  if (falaDoCliente) {
    st.historico.push({ role: "user", content: falaDoCliente });
    st.transcricao.push(`Cliente: ${falaDoCliente}`);
  }
  st.historico.push({ role: "assistant", content: texto });
  st.transcricao.push(`Agente: ${texto}`);
  st.falaAtual = texto;
  const pergunta = ehPergunta(texto);
  st.turnoEmPergunta = pergunta;
  L(st, `[sistema] "${texto}"`);
  await falarComMinhaVoz(ws, st, texto, meu);
  if (st.turno !== meu) return;
  if (pergunta && !encerrar) st.ultimaPergunta = texto;
  turnoEnviado(st, meu);
  if (encerrar && st.encerrarAuto) {
    for (let i = 0; i < 60 && st.falando && st.turno === meu; i++) await pausa(250);
    await pausa(1200);
    if (st.turno === meu) await desligar(st, "número não confirmado");
  }
}

// Travas aplicadas a cada pedaço, antes de falar
function aplicarTravas(st, trecho) {
  let t = trecho;

  // WhatsApp: pedido de número liga o modo ditado, com limite de tentativas
  if (PEDE_NUMERO.test(t)) {
    st.pedidosNumero++;
    if (st.pedidosNumero > MAX_TENTATIVAS_NUMERO) {
      LW(st, `[roteiro] 🚫 já pediu o número ${MAX_TENTATIVAS_NUMERO}x — saída segura`);
      return { texto: SAIDA_SEGURA_NUMERO, encerrar: true };
    }
    st.modoDitado = true;
    st.ditadoAte = Date.now() + DITADO_JANELA_MS;
    st.confirmandoNumero = false;
    L(st, `[roteiro] 🎙️ pediu o número (tentativa ${st.pedidosNumero}/${MAX_TENTATIVAS_NUMERO}) — modo ditado ligado`);
  }

  // Nome: natural, no máximo N por resposta (e opcionalmente N por ligação)
  if (st.nomeCliente) {
    const corrigido = corrigirNome(t, st.nomeCliente, st.palavrasProtegidas);
    if (corrigido !== t) { L(st, `[roteiro] 👤 nome corrigido: "${t}" → "${corrigido}"`); t = corrigido; }
    const n = contarNome(t, st.nomeCliente);
    if (n > 0) {
      const estourouTurno = st.nomeNoTurno >= MAX_NOME_POR_TURNO;
      const estourouLigacao = MAX_USOS_NOME > 0 && st.usosNome >= MAX_USOS_NOME;
      if (estourouTurno || estourouLigacao) {
        t = tirarNome(t, st.nomeCliente);
        L(st, `[roteiro] 👤 nome retirado (${estourouTurno ? "já usado nesta resposta" : "limite da ligação"})`);
      } else {
        if (n > 1) t = manterSoPrimeira(t, st.nomeCliente);
        st.nomeNoTurno++; st.usosNome++;
      }
    }
  }
  return { texto: t, encerrar: false };
}

// ----- WhatsApp: o motor cuida do número (o Claude não converte dígitos) -----
function reperguntarNumero(ws, st, texto, prefixo) {
  if (st.pedidosNumero >= MAX_TENTATIVAS_NUMERO) {
    LW(st, `[whatsapp] 🚫 já pedi o número ${st.pedidosNumero}x — saída segura`);
    st.confirmandoNumero = false;
    falarFixo(ws, st, texto, SAIDA_SEGURA_NUMERO, { encerrar: true })
      .catch((e) => LE(st, `[sistema] erro: ${e?.message || e}`));
    return;
  }
  st.pedidosNumero++;
  st.modoDitado = true;
  st.ditadoAte = Date.now() + DITADO_JANELA_MS;
  L(st, `[roteiro] 🎙️ pedi o número de novo (tentativa ${st.pedidosNumero}/${MAX_TENTATIVAS_NUMERO}) — modo ditado ligado`);
  falarFixo(ws, st, texto, `${prefixo} Me fala de novo o WhatsApp com DDD, bem devagar?`)
    .catch((e) => LE(st, `[sistema] erro: ${e?.message || e}`));
}

// Devolve true quando o próprio motor respondeu (sem chamar o Claude)
function tratarNumero(ws, st, texto, eraDitado) {
  const digitos = palavrasParaDigitos(texto);
  const num = validarTelefone(digitos, dddDoTelefone(st.contato?.telefone));

  // 1) Número completo e válido → o motor lê de volta
  if (num && (eraDitado || st.confirmandoNumero || digitos.length >= 9)) {
    st.numeroEntendido = num; st.confirmandoNumero = true; st.whatsappConfirmado = "";
    L(st, `[whatsapp] 📱 entendi ${formatarTelefone(num)} ("${digitos}") — lendo de volta`);
    falarFixo(ws, st, texto, `Anotei aqui: ${lerTelefone(num)}. Tá certo?`)
      .catch((e) => LE(st, `[sistema] erro: ${e?.message || e}`));
    return true;
  }
  // 2) Estava ditando, mas veio número incompleto → pede de novo
  if (eraDitado && digitos.length >= 3) {
    LW(st, `[whatsapp] ⚠️ número incompleto: "${digitos}" (${digitos.length} dígitos)`);
    reperguntarNumero(ws, st, texto, "Acho que faltou algum número.");
    return true;
  }
  // 3) Resposta à leitura do número
  if (st.confirmandoNumero) {
    const r = classificarConfirmacao(texto);
    st.confirmandoNumero = false;
    if (r === "sim") {
      st.whatsappConfirmado = st.numeroEntendido;
      L(st, `[whatsapp] ✅ confirmado: ${formatarTelefone(st.whatsappConfirmado)}`);
      return false;   // o Claude se despede
    }
    if (r === "nao") {
      LW(st, "[whatsapp] ❌ a pessoa disse que o número está errado");
      reperguntarNumero(ws, st, texto, "Opa, desculpa.");
      return true;
    }
  }
  return false;
}

async function pensarEResponder(ws, st, falaDoCliente) {
  if (!falaDoCliente) return;
  if (!anthropic) { LE(st, "[cerebro] ANTHROPIC_API_KEY não configurada"); return; }
  const meuTurno = iniciarTurno(st);
  st.claudePensando = true;
  st.tentativasResgate = 0;
  st.nomeNoTurno = 0;
  st.ultimaPergunta = ""; st.jaRepetiuPergunta = false;
  st.parcial = { turno: meuTurno, texto: "" };

  const inicio = Date.now();
  let primeiraEm = 0, buffer = "", pendente = "";
  let acabouTexto = false, pediuFim = false, cortou = false, travou = false, falhou = false;
  let stream = null, cao = null;
  const fila = [];      // { texto, frase } — frase = a pergunta inteira de onde o pedaço saiu
  const falado = [];

  const limparMarca = (t) => {
    if (/\[FIM\]/i.test(t)) pediuFim = true;
    if (/\[(AGUARD|ESPER)[^\]]*\]/i.test(t)) entrarEmEspera(st, "o Claude marcou espera");
    return semMarcadores(t);
  };
  const empurrar = (t) => {
    if (cortou) return;
    const limpo = limparMarca(t);
    if (!limpo) return;
    pendente = pendente ? pendente + " " + limpo : limpo;
    const minimo = fila.length === 0 ? MIN_FALA_PRIMEIRA : MIN_FALA_RESTO;
    const pergunta = ehPergunta(limpo);
    if (pendente.length >= minimo || (UMA_PERGUNTA && pergunta)) {
      const partes = quebrarSeLonga(pendente);
      partes.forEach((p, k) => fila.push({ texto: p, frase: pergunta && k === partes.length - 1 ? limpo : "" }));
      pendente = "";
    }
  };

  try {
    st.historico.push({ role: "user", content: falaDoCliente });
    st.transcricao.push(`Cliente: ${falaDoCliente}`);

    stream = anthropic.messages.stream({
      model: CLAUDE_MODEL, max_tokens: 110, system: st.persona, messages: sanitizar(st.historico),
    });
    st.streamClaude = stream;
    cao = setTimeout(() => {
      if (!primeiraEm && st.turno === meuTurno) {
        travou = true;
        LW(st, `[cerebro] ⚠️ Claude não respondeu em ${CLAUDE_TIMEOUT_MS}ms — abortando`);
        try { stream.abort(); } catch {}
      }
    }, CLAUDE_TIMEOUT_MS);

    stream.on("text", (d) => {
      buffer += d;
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
          const item = fila[i++];
          const { texto: trecho, encerrar } = aplicarTravas(st, item.texto);
          if (!trecho) continue;
          if (!primeiraEm) {
            primeiraEm = Date.now() - inicio;
            L(st, `[cerebro] ⚡ primeira fala em ${primeiraEm}ms: "${trecho}"`);
          } else L(st, `[cerebro] continua: "${trecho}"`);
          falado.push(trecho);
          if (st.parcial?.turno === meuTurno) st.parcial.texto = juntarTexto(st.parcial.texto, trecho);
          st.falaAtual = juntarTexto(st.falaAtual, trecho);
          const pergunta = ehPergunta(trecho);
          const social = pergunta && ehPerguntaSocial(item.frase || trecho, st.nomeCliente);
          st.turnoEmPergunta = pergunta && !social;
          await falarComMinhaVoz(ws, st, trecho, meuTurno);
          if (st.turno === meuTurno) st.ultimaPergunta = pergunta && !social && !encerrar ? (item.frase || trecho) : "";

          if (encerrar) {
            pediuFim = true; cortou = true;
            if (st.streamClaude === stream) { try { stream.abort(); } catch {} }
            return;
          }
          if (UMA_PERGUNTA && pergunta && !social) {
            cortou = true;
            try { stream.abort(); } catch {}
            if (i < fila.length || !acabouTexto) L(st, "[roteiro] ✂️ parei na pergunta — uma por vez");
            return;
          }
        } else if (acabouTexto) return;
        else await pausa(50);
      }
      L(st, "[cerebro] turno abortado");
    })();

    try {
      await stream.finalMessage();
    } catch (e) {
      if (!ehAbortado(e)) { falhou = true; LE(st, `[cerebro] erro: ${e?.message || e}`); }
    }
    clearTimeout(cao);
    if (!cortou && !falhou && !travou) {
      const resto = limparMarca((pendente + " " + buffer).trim());
      if (resto) for (const p of quebrarSeLonga(resto)) fila.push({ texto: p, frase: "" });
    }
    pendente = ""; buffer = "";
    acabouTexto = true;
    if (st.turno === meuTurno) st.claudePensando = false;
    await consumidor;

    const dito = falado.join(" ").trim();
    if (st.turno === meuTurno && dito) {
      st.historico.push({ role: "assistant", content: dito });
      st.transcricao.push(`Agente: ${dito}`);
      if (st.parcial?.turno === meuTurno) st.parcial = null;
      L(st, `[cerebro] turno completo em ${Date.now() - inicio}ms`);
    }

    if (pediuFim && st.encerrarAuto && st.turno === meuTurno) {
      st.fimPendente = true;
      st.ultimaPergunta = "";
      L(st, "[fim] encerramento — aguardando a fala terminar");
      turnoEnviado(st, meuTurno);
      for (let i = 0; i < 60 && st.falando && st.turno === meuTurno; i++) await pausa(250);
      await pausa(1200);
      if (st.turno === meuTurno) await desligar(st, "objetivo cumprido");
    }
  } catch (e) {
    if (!ehAbortado(e)) {
      falhou = true;
      LE(st, `[cerebro] erro: ${e?.message || e}`);
    }
  } finally {
    clearTimeout(cao);
    // Só mexe no estado se este ainda for o turno atual (turno antigo não apaga o novo)
    if (st.turno === meuTurno) {
      st.claudePensando = false;
      if (st.streamClaude === stream) st.streamClaude = null;
      turnoEnviado(st, meuTurno);
    } else if (stream && st.streamClaude === stream) st.streamClaude = null;
  }

  // Claude travou ou deu erro e nada foi falado: não deixa a pessoa no vácuo
  if ((falhou || travou) && st.turno === meuTurno && falado.length === 0 && !st.encerrando) {
    await falarAvulso(ws, st, FRASE_FALHA, "falha");
  }
}

wssStreams.on("connection", (ws) => {
  const st = {
    tag: "", iniciadoEm: Date.now(),
    streamSid: null, callSid: null, pacotes: 0,
    dg: null, dgPronto: false, dgRequestId: "", dgReconexoes: 0, dgZeroEm: 0,
    fila: [], filaDesde: 0,
    balde: "", ultimoInterim: "", flushTimer: null, flushAte: 0, repeticoes: 0,
    corteAudio: 0, fimBalde: 0, fimInterim: 0,
    jaDespachado: "", despachadoEm: 0,
    historico: [], transcricao: [],
    turno: 0, falando: false, claudePensando: false, marcasPendentes: 0,
    turnoPronto: false, turnoEmPergunta: false, respostaNaFila: "", fimPrevisto: 0,
    podeInterromperApos: 0, streamClaude: null,
    calado_desde: Date.now(), esperandoDesde: 0, vozEm: 0, vozLogEm: 0,
    ultimaPergunta: "", jaRepetiuPergunta: false,
    falaAtual: "", falaAnterior: "",
    tentativasResgate: 0, vigiaOcupado: false,
    encerrando: false, despedindo: false, fimPendente: false, fechado: false,
    campanhaId: "", contatoId: "", agenteId: "",
    agente: null, contato: null, supabase: null,
    persona: "", saudacao: "", vozId: "", velocidade: 1.0,
    vozSettings: { stability: 0.4, similarity_boost: 0.8, style: 0.45 },
    nivelVoz: 0, encerrarAuto: true, fraseDespedida: "", silencioMs: SILENCIO_PADRAO_MS,
    nomeCliente: "", usosNome: 0, nomeNoTurno: 0,
    modoDitado: false, ditadoAte: 0, pedidosNumero: 0,
    numeroEntendido: "", confirmandoNumero: false, whatsappConfirmado: "",
    caixaPostal: false, motivoSemConversa: "", naoLigarMais: false,
    // FASE 12.2: abertura, espera, assistente de chamadas, falas interrompidas
    aguardandoAlo: false, primeiraVozEm: 0, ultimaAtividadeEm: 0, ultimoTextoEm: 0, ouviuAlo: "",
    esperaAte: 0, triagem: false, triagemFeita: false,
    parcial: null, palavrasProtegidas: null, algumaVoz: false,
    fimDaFala: null,
  };
  L(st, "[streams] túnel aberto, aguardando áudio…");

  function limparFlush() {
    if (st.flushTimer) { clearTimeout(st.flushTimer); st.flushTimer = null; }
    st.flushAte = 0;
  }

  function emDitado() {
    if (st.modoDitado && Date.now() > st.ditadoAte) {
      st.modoDitado = false;
      L(st, "[ouvido] 🎙️ modo ditado expirou");
    }
    return st.modoDitado;
  }

  // Junta o que já fechou + o que ainda está em andamento
  function falaAcumulada() {
    const b = (st.balde || "").trim();
    const i = (st.ultimoInterim || "").trim();
    if (!i) return b;
    if (!b) return i;
    if (b.endsWith(i)) return b;
    return (b + " " + i).replace(/\s{2,}/g, " ").trim();
  }

  // Marca o áudio até aqui como "já usado": o que o Deepgram reenviar dele é ignorado
  function consumirAudio() {
    const fim = Math.max(st.fimBalde || 0, st.fimInterim || 0);
    if (fim > st.corteAudio) st.corteAudio = fim;
    st.fimBalde = 0; st.fimInterim = 0;
  }

  // Só as palavras NOVAS (depois do corte). Some o eco do Deepgram, fica a fala nova.
  function textoNovo(alt) {
    const bruto = String(alt?.transcript || "").trim();
    if (!bruto) return { texto: "", fim: 0, semTempo: false };
    const palavras = Array.isArray(alt.words) ? alt.words : [];
    if (!palavras.length) return { texto: bruto, fim: 0, semTempo: true };
    const novas = palavras.filter((w) => ((Number(w.start) || 0) + (Number(w.end) || 0)) / 2 > st.corteAudio);
    if (!novas.length) return { texto: "", fim: 0, semTempo: false };
    const fim = Math.max(...novas.map((w) => Number(w.end) || 0));
    if (novas.length === palavras.length) return { texto: bruto, fim, semTempo: false };
    let texto = novas.map((w) => w.punctuated_word || w.word || "").join(" ").replace(/\s{2,}/g, " ").trim();
    texto = texto.charAt(0).toUpperCase() + texto.slice(1);
    return { texto, fim, semTempo: false };
  }

  // Quanto o Deepgram está atrasado em relação ao áudio real
  function atrasoDoOuvido(ev) {
    if (!st.dgZeroEm) return 0;
    const fimAudio = (Number(ev.start) || 0) + (Number(ev.duration) || 0);
    return (Date.now() - st.dgZeroEm) / 1000 - fimAudio;
  }

  // A pessoa respondeu nos últimos segundos da PERGUNTA do Carlos: guarda a resposta
  function podeGuardar(texto) {
    const faltaMs = (st.fimPrevisto || 0) - Date.now();
    return st.turnoPronto && st.turnoEmPergunta && !st.claudePensando && !st.fimPendente &&
      faltaMs <= JANELA_RESPOSTA_MS && !ehEcoProvavel(st, texto);
  }

  function despachar(fala, motivo) {
    limparFlush();
    const texto = (fala || "").trim();
    consumirAudio();
    st.balde = ""; st.ultimoInterim = ""; st.repeticoes = 0;
    if (!texto || st.encerrando || st.despedindo) return;
    // Abertura: o "alô" da pessoa não vai pro Claude — quem responde é a saudação
    if (st.aguardandoAlo) {
      st.ouviuAlo = juntarTexto(st.ouviuAlo, texto);
      L(st, `[abertura] 👋 ouvi: "${texto}"`);
      return;
    }
    // Assistente de chamadas: só uma pessoa de verdade ("Alô?", "Oi", "Quem é?") tira da espera
    if (st.triagem) {
      if (ehAloHumano(texto)) sairDaTriagem(texto);
      else L(st, `[triagem] (ignorado, assistente ainda na linha) "${texto}"`);
      return;
    }
    if (st.falando || st.claudePensando || st.marcasPendentes > 0) {
      if (podeGuardar(texto)) {
        st.respostaNaFila = juntarTexto(st.respostaNaFila, texto);
        L(st, `[ouvido] ⏳ respondeu antes de eu terminar — guardei: "${st.respostaNaFila}"`);
      } else {
        L(st, `[ouvido] (ignorado, ainda falando) "${texto}"`);
      }
      return;
    }
    if (st.esperandoDesde && Date.now() - st.esperandoDesde < 2500 && ehEcoProvavel(st, texto)) {
      L(st, `[ouvido] (eco da minha própria voz, ignorado) "${texto}"`);
      return;
    }
    const eraDitado = st.modoDitado;
    if (st.modoDitado) {
      st.modoDitado = false;
      L(st, `[ouvido] 🎙️ ditado recebido: "${texto}" — modo ditado desligado`);
    }
    st.jaDespachado = texto;
    st.despachadoEm = Date.now();
    L(st, `[ouvido] >>> pessoa disse: "${texto}"  [${motivo}]`);
    if (!st.naoLigarMais && pediuNaoLigar(texto)) {
      st.naoLigarMais = true;
      L(st, "[contato] 🚫 pediu pra não ligar mais — vai ficar como \"Não atender\" no painel");
    }
    if (tratarNumero(ws, st, texto, eraDitado)) return;
    pensarEResponder(ws, st, texto).catch((e) => LE(st, `[cerebro] erro inesperado: ${e?.message || e}`));
  }

  // O último áudio do turno terminou de tocar
  function fimDaFala() {
    st.falando = false;
    if (st.fechado) return;
    const agora = Date.now();
    st.calado_desde = agora; st.esperandoDesde = agora;
    L(st, "[streams] terminou de falar — escutando 👂");
    if (st.fimPendente) { st.despedindo = true; return; }
    if (st.respostaNaFila) {
      const r = st.respostaNaFila;
      st.respostaNaFila = "";
      if (st.balde || st.ultimoInterim) {
        // a pessoa ainda está falando: junta e deixa o tempo decidir
        st.balde = juntarTexto(r, st.balde);
        if (!st.flushTimer) agendarFlush(FLUSH_CURTO_MS);
      } else {
        despachar(r, "respondeu no fim da pergunta");
      }
    }
  }
  st.fimDaFala = fimDaFala;

  function agendarFlush(ms, soSeMaisCedo = false) {
    const alvo = Date.now() + ms;
    if (soSeMaisCedo && st.flushTimer && st.flushAte <= alvo) return;
    if (st.flushTimer) clearTimeout(st.flushTimer);
    st.flushAte = alvo;
    st.flushTimer = setTimeout(() => {
      st.flushTimer = null; st.flushAte = 0;
      const fala = falaAcumulada();
      if (fala) despachar(fala, st.modoDitado ? "fim do ditado" : "fechado por tempo");
    }, ms);
  }

  // Máquina no lugar da pessoa (piloto 28/09): caixa postal, recado da operadora, assistente
  // de chamadas. Caixa/recado/"indisponível": cala, desliga e NÃO conta como conversa — o
  // contato volta pra fila. Assistente pedindo nome e motivo: o Carlos se apresenta e espera.
  // Devolve true quando o texto era da máquina (não segue pro Claude).
  function pegouMaquina(texto) {
    if (st.caixaPostal) return true;
    if (!CAIXA_POSTAL || st.encerrando || st.fechado) return false;
    if (CAIXA_POSTAL_JANELA_S > 0 && Date.now() - st.iniciadoEm > CAIXA_POSTAL_JANELA_S * 1000) return false;
    let tipo = tipoDeMaquina(texto);
    if (!tipo && st.balde) tipo = tipoDeMaquina(juntarTexto(st.balde, texto));
    if (!tipo) {
      if (st.esperaAte && !st.triagem && ehAloHumano(texto)) {
        st.esperaAte = 0;
        L(st, `[espera] a pessoa voltou: "${texto}"`);
      }
      return false;
    }
    if (tipo === "espera") {
      if (st.triagem) { entrarEmEspera(st, `assistente: "${texto}"`, TRIAGEM_MAX_MS); return true; }
      entrarEmEspera(st, `pediram pra aguardar: "${texto}"`);
      return false;   // gente de verdade: o Claude responde "Tudo bem, eu aguardo."
    }
    if (tipo === "triagem") {
      if (st.triagemFeita) return true;                                     // o assistente repetindo
      if (st.historico.some((m) => m.role === "user")) return false;         // já era conversa de verdade
      entrarNaTriagem(texto);
      return true;
    }
    st.caixaPostal = true;
    st.motivoSemConversa = MOTIVO_MAQUINA[tipo] || "Caixa postal";
    L(st, `[maquina] 📭 ${st.motivoSemConversa}: "${texto}" — desligando (conta como não atendeu)`);
    st.aguardandoAlo = false;
    calarABoca(ws, st, st.motivoSemConversa);
    limparFlush(); st.balde = ""; st.ultimoInterim = ""; st.repeticoes = 0;
    st.transcricao.push(`(${st.motivoSemConversa} — desligado pelo motor: "${texto}")`);
    desligar(st, st.motivoSemConversa).catch(() => {});
    return true;
  }

  // "Aqui é o Carlos, da Invite Energy." (tirado da saudação) + motivo
  function fraseDeTriagem() {
    const m = String(st.saudacao || "").match(/aqui (é|e) [^.!?]+/i);
    const quem = m ? m[0].trim().replace(/^./, (c) => c.toUpperCase()) + "." : "";
    return juntarTexto(quem, FRASE_MOTIVO_TRIAGEM);
  }
  function entrarNaTriagem(texto) {
    st.triagem = true; st.triagemFeita = true; st.aguardandoAlo = false;
    L(st, `[triagem] 🤖 assistente de chamadas: "${texto}" — me apresento e espero calado`);
    calarABoca(ws, st, "assistente de chamadas");
    limparFlush(); st.balde = ""; st.ultimoInterim = ""; st.repeticoes = 0;
    st.transcricao.push(`Assistente do celular: ${texto}`);
    entrarEmEspera(st, "assistente de chamadas", TRIAGEM_MAX_MS);
    falarAvulso(ws, st, fraseDeTriagem(), "triagem")
      .then(() => { st.ultimaPergunta = ""; st.jaRepetiuPergunta = true; })
      .catch((e) => LE(st, `[triagem] erro: ${e?.message || e}`));
  }
  function sairDaTriagem(texto) {
    st.triagem = false; st.esperaAte = 0;
    L(st, `[triagem] 🙋 a pessoa atendeu: "${texto}" — repito a saudação`);
    if (st.falando || st.marcasPendentes > 0) calarABoca(ws, st, "a pessoa atendeu");
    st.transcricao.push(`Cliente: ${texto}`);
    falarAvulso(ws, st, st.saudacao, "saudacao")
      .then((meu) => {
        if (st.turno === meu) { st.ultimaPergunta = ultimaFrasePergunta(st.saudacao); st.jaRepetiuPergunta = false; }
      })
      .catch((e) => LE(st, `[triagem] erro: ${e?.message || e}`));
  }

  function aoDetectarVoz() {
    const agora = Date.now();
    st.ultimaAtividadeEm = agora; st.algumaVoz = true;
    if (!st.primeiraVozEm) st.primeiraVozEm = agora;
    if (st.falando || st.claudePensando || st.marcasPendentes > 0) return;
    st.vozEm = agora;
    if (agora - st.vozLogEm > 3000) { st.vozLogEm = agora; L(st, "[ouvido] 🗣️ voz detectada"); }
  }

  function abrirOuvido() {
    if (!DEEPGRAM_API_KEY) { LE(st, "[deepgram] chave não configurada!"); return; }
    const idioma = st.agente?.idioma || "pt-BR";
    let url =
      `${DG_URL}?encoding=mulaw&sample_rate=8000&channels=1` +
      `&language=${encodeURIComponent(idioma)}&model=${encodeURIComponent(DG_MODEL)}` +
      `&punctuate=true&smart_format=${DG_SMART_FORMAT ? "true" : "false"}` +
      `&interim_results=true&endpointing=${DG_ENDPOINTING}&utterance_end_ms=1000&vad_events=true`;
    if (DG_MODEL.startsWith("nova-3")) {
      for (const k of DG_KEYTERMS) url += `&keyterm=${encodeURIComponent(k)}`;
    }
    const dg = new WebSocket(url, { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } });
    st.dg = dg;

    dg.on("upgrade", (res) => { st.dgRequestId = String(res?.headers?.["dg-request-id"] || ""); });

    dg.on("open", () => {
      if (st.dg !== dg) return;
      st.dgPronto = true;
      st.dgZeroEm = st.fila.length ? st.filaDesde : Date.now();
      st.corteAudio = 0; st.fimBalde = 0; st.fimInterim = 0;   // conexão nova: relógio do áudio recomeça
      L(st, `[deepgram] ouvido conectado ✅ (${DG_MODEL})${st.dgRequestId ? ` id ${st.dgRequestId}` : ""}`);
      for (const b of st.fila) { try { dg.send(b); } catch {} }
      st.fila = [];
    });

    dg.on("unexpected-response", (req, res) => {
      LE(st, `[deepgram] ❌ recusou a conexão (HTTP ${res.statusCode}) — confira DG_MODEL e o idioma`);
      try { req.destroy(); } catch {}
    });

    dg.on("message", (raw) => {
      let ev;
      try { ev = JSON.parse(raw.toString()); } catch { return; }

      if (ev.type === "SpeechStarted") { aoDetectarVoz(); return; }

      if (ev.type === "UtteranceEnd") {
        if (emDitado()) return;   // no ditado a pessoa pausa entre os grupos
        const fala = falaAcumulada();
        if (fala) despachar(fala, "UtteranceEnd");
        return;
      }
      if (ev.type !== "Results") return;
      const { texto, fim, semTempo } = textoNovo(ev.channel?.alternatives?.[0]);
      if (!texto) return;
      st.ultimaAtividadeEm = st.ultimoTextoEm = Date.now(); st.algumaVoz = true;
      if (!st.primeiraVozEm) st.primeiraVozEm = st.ultimaAtividadeEm;
      if (pegouMaquina(texto)) return;
      // sem os tempos das palavras, volta pro filtro antigo (mesmo texto em 5s)
      if (semTempo && texto === st.jaDespachado && Date.now() - st.despachadoEm < ECO_MS) return;

      st.calado_desde = Date.now();
      st.tentativasResgate = 0;
      const estaFalando = st.falando || st.marcasPendentes > 0 || st.claudePensando;

      const triagemMaquina = st.triagem && !ehAloHumano(texto);   // assistente falando: não interrompe o Carlos
      if (estaFalando && texto.length >= BARGE_MIN_CHARS && !st.encerrando && !st.despedindo && !triagemMaquina) {
        if (ehEcoProvavel(st, texto)) {
          L(st, `[barge-in] (eco da minha própria voz, ignorado) "${texto}"`);
        } else if (Date.now() > st.podeInterromperApos) {
          calarABoca(ws, st, `pessoa disse: "${texto}"`);
          limparFlush(); st.balde = ""; st.ultimoInterim = ""; st.repeticoes = 0;
          st.fimBalde = 0; st.fimInterim = 0;
        } else L(st, `[barge-in] (carência) "${texto}"`);
      }

      const ditado = emDitado();

      if (!ev.is_final) {
        st.fimInterim = fim;
        // MODO DITADO: deixa a pessoa pausar entre os grupos do número
        if (ditado) {
          if (texto !== st.ultimoInterim) {
            L(st, `[ouvido] 🎙️ ditado… "${texto}"`);
            st.ultimoInterim = texto; st.repeticoes = 0;
            agendarFlush(FLUSH_DITADO_MS);
          }
          return;
        }
        if (texto === st.ultimoInterim) {
          st.repeticoes++;
          if (st.repeticoes === 1) {
            L(st, `[ouvido] texto estável ("${texto}") — fechando em ${FLUSH_REPETIDO_MS}ms`);
            agendarFlush(FLUSH_REPETIDO_MS, true);
          }
          return;
        }
        const atrasoParcial = st.ultimoInterim ? 0 : atrasoDoOuvido(ev);
        L(st, `[ouvido] ouvindo… "${texto}"` +
          (atrasoParcial >= 1.5 ? `  ⚠️ ouvido atrasado ${atrasoParcial.toFixed(1)}s` : ""));
        st.ultimoInterim = texto;
        st.repeticoes = 0;
        if (ehRespostaCurta(texto)) {
          L(st, `[ouvido] ⚡ resposta curta ("${texto}") — fechando em ${FLUSH_CURTO_MS}ms`);
          agendarFlush(FLUSH_CURTO_MS);
        } else if (ehValorCompleto(texto)) {
          L(st, `[ouvido] ⚡ valor completo ("${texto}") — fechando em ${FLUSH_VALOR_MS}ms`);
          agendarFlush(FLUSH_VALOR_MS);
        } else {
          agendarFlush(FLUSH_MS * 2);
        }
        return;
      }

      const atraso = atrasoDoOuvido(ev);
      L(st, `[ouvido] FINAL: "${texto}"${ev.speech_final ? "  <-- terminou" : ""}` +
        (atraso >= 1.5 ? `  ⚠️ ouvido atrasado ${atraso.toFixed(1)}s` : ""));
      st.balde = juntarTexto(st.balde, texto);
      st.fimBalde = Math.max(st.fimBalde, fim);
      st.ultimoInterim = ""; st.fimInterim = 0; st.repeticoes = 0;

      if (ditado) { agendarFlush(FLUSH_DITADO_MS); return; }   // ignora o "terminou" no ditado
      if (ev.speech_final) despachar(st.balde, "speech_final");
      else if (ehRespostaCurta(st.balde)) agendarFlush(FLUSH_CURTO_MS, true);
      else if (ehValorCompleto(st.balde)) agendarFlush(FLUSH_VALOR_MS, true);
      else agendarFlush(FLUSH_MS, true);
    });

    dg.on("error", (e) => LE(st, `[deepgram] erro: ${e?.message}`));
    dg.on("close", (c) => {
      if (st.dg !== dg) return;
      st.dgPronto = false;
      L(st, `[deepgram] desconectado (código ${c})`);
      // Caiu no meio da ligação: reconecta (senão o Carlos fica surdo até o fim)
      if (!st.fechado && !st.encerrando && st.dgReconexoes < 3) {
        st.dgReconexoes++;
        LW(st, `[deepgram] ⚠️ caiu no meio da ligação — reconectando (${st.dgReconexoes}/3)`);
        setTimeout(() => { if (!st.fechado && !st.encerrando) abrirOuvido(); }, 300);
      }
    });
  }

  // Abertura humana: quem atende costuma dizer "Alô?" — o Carlos responde depois disso, em
  // vez de atropelar. Ninguém falou em ALO_SILENCIO_MS → começa assim mesmo. Se a "pessoa"
  // for máquina (caixa postal, recado, assistente), ela se revela aqui, antes do Carlos falar.
  async function esperarAlo() {
    if (!ESPERAR_ALO) return true;
    const livre = () => !st.fechado && !st.encerrando && !st.triagem && !st.caixaPostal;
    st.aguardandoAlo = true;
    const t0 = Date.now();
    let motivo = "limite de espera";
    while (st.aguardandoAlo && livre()) {
      const agora = Date.now();
      if (agora - t0 >= ALO_MAX_MS) break;
      if (!st.primeiraVozEm && agora - t0 >= ALO_SILENCIO_MS) { motivo = "ninguém falou"; break; }
      if (st.primeiraVozEm) {
        // Voz detectada mas o texto ainda não veio: o "Alô?" está a caminho — espera ele
        // (teste de 28/09: o Carlos começou 0,7s depois da voz e atropelou o "Alô?")
        const semTexto = !st.ultimoTextoEm;
        if (agora - st.ultimaAtividadeEm >= (semTexto ? ALO_VOZ_SEM_TEXTO_MS : ALO_PAUSA_MS)) {
          motivo = semTexto ? "ouvi voz, sem texto" : "a pessoa falou";
          break;
        }
      }
      await pausa(40);
    }
    const segue = st.aguardandoAlo && livre();
    st.aguardandoAlo = false;
    if (!segue) return false;
    const ouvido = juntarTexto(st.ouviuAlo, falaAcumulada());
    // o "alô" já foi respondido pela saudação: nada dele sobra pro Claude
    limparFlush(); consumirAudio();
    st.balde = ""; st.ultimoInterim = ""; st.repeticoes = 0;
    L(st, `[abertura] ${ouvido ? `👋 ouvi "${ouvido}"` : motivo === "ninguém falou" ? "🤫 ninguém falou" : `🗣️ ${motivo}`}` +
      ` — saudação depois de ${Date.now() - t0}ms`);
    return true;
  }

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
        const [ag, ct] = await Promise.all([
          carregarAgente(sb.client, st.agenteId),
          carregarContato(sb.client, st.contatoId),
        ]);
        st.agente = ag; st.contato = ct;
      } catch (e) { LE(st, `[banco] erro ao carregar contexto: ${e?.message}`); }
    }

    if (st.agente) L(st, `[agente] "${st.agente.nome}" carregado do painel em ${Date.now() - t0}ms`);
    else LW(st, "[agente] ⚠️ nenhum agente carregado — usando padrão genérico");

    st.nomeCliente = primeiroNome(st.contato?.nome || "");
    if (st.contato?.nome) {
      L(st, `[contato] falando com: ${st.contato.nome}` +
        (st.nomeCliente ? ` (vai chamar de "${st.nomeCliente}")` : " (nome não será falado)"));
    }

    st.saudacao = textoSaudacao(st.agente, st.contato);
    st.persona = montarPersonaStreams(st.agente, st.contato, st.saudacao);
    const cfg = configVoz(st.agente);
    st.vozId = cfg.vozId; st.velocidade = cfg.velocidade; st.vozSettings = cfg.settings;
    st.encerrarAuto = st.agente?.encerrar_automaticamente !== false;
    st.fraseDespedida = String(st.agente?.frase_despedida || "").trim() ||
      "Obrigado pela atenção e tenha um ótimo dia!";
    const segSilencio = parseInt(st.agente?.silencio_para_encerrar_segundos, 10);
    st.silencioMs = isFinite(segSilencio) && segSilencio > 0 ? segSilencio * 1000 : SILENCIO_PADRAO_MS;

    L(st,
      `[agente] voz ${st.vozId} | vel ${st.velocidade}x | ` +
      `estab ${st.vozSettings.stability} · simil ${st.vozSettings.similarity_boost} · estilo ${st.vozSettings.style} ` +
      `| silêncio ${Math.round(st.silencioMs / 1000)}s | encerrar auto: ${st.encerrarAuto ? "sim" : "não"}`
    );
    if (st.saudacao.length > 160) {
      LW(st, `[agente] ⚠️ saudação longa (${st.saudacao.length} chars) — encurte no painel`);
    }
    if (/[%]|\d/.test(st.saudacao)) {
      LW(st, `[agente] ⚠️ saudação tem algarismo ou "%" — escreva por extenso no painel`);
    }

    abrirOuvido();
    aquecerCerebro(st);

    // Palavras da saudação e do agente (menos o nome do cliente) nunca viram "nome corrigido"
    const nomeNorm = normalizar(st.nomeCliente);
    st.palavrasProtegidas = new Set(normalizar(`${st.saudacao} ${st.agente?.nome || ""}`)
      .split(" ").filter((w) => w && w !== nomeNorm));

    const tS = Date.now();
    const { promessa, doCache } = obterSaudacao(cfg, st.saudacao, st);   // já vai buscando o áudio

    // Espera o "alô" (ou a máquina se revelar) antes de falar
    if (!(await esperarAlo())) return;

    const meuTurno = iniciarTurno(st);
    st.podeInterromperApos = Date.now() + 1500;
    st.calado_desde = Date.now();
    st.transcricao.push(`Agente: ${st.saudacao}`);
    st.falaAtual = st.saudacao;
    st.turnoEmPergunta = ehPergunta(st.saudacao);
    if (st.nomeCliente && contarNome(st.saudacao, st.nomeCliente) > 0) st.usosNome++;

    const audio = await promessa;
    L(st, `[saudacao] ${doCache ? "🎯 veio do cache" : "gerada agora"} — pronta em ${Date.now() - tS}ms` +
      (audio ? ` (${(audio.length / 8000).toFixed(1)}s)` : ""));
    if (audio) await enviarAudio(ws, st, audio, meuTurno);
    if (st.turno === meuTurno) {
      st.ultimaPergunta = ultimaFrasePergunta(st.saudacao);
      st.jaRepetiuPergunta = false;
    }
    turnoEnviado(st, meuTurno);
  }

  const vigia = setInterval(async () => {
    if (st.fechado || st.encerrando || st.despedindo || !st.persona || st.vigiaOcupado || st.aguardandoAlo) return;
    const ditado = emDitado();
    const ocupado = st.falando || st.claudePensando || st.marcasPendentes > 0;
    if (ocupado || st.balde || st.ultimoInterim) { st.calado_desde = Date.now(); return; }
    const agora = Date.now();

    // 0) Em espera ("permaneça na linha", "aguarde", assistente de chamadas): calado até o prazo
    if (st.esperaAte) {
      if (agora < st.esperaAte) { st.calado_desde = agora; return; }
      st.esperaAte = 0;
      if (st.triagem) {
        st.motivoSemConversa = "Assistente de chamadas: ninguém atendeu";
        L(st, "[triagem] ⌛ o assistente não passou a ligação — desligando (tenta de novo depois)");
        st.vigiaOcupado = true;
        try { await desligar(st, "assistente de chamadas sem retorno"); } finally { st.vigiaOcupado = false; }
        return;
      }
      L(st, "[espera] ⌛ acabou o tempo de espera");
      st.calado_desde = agora - st.silencioMs;   // chama "Alô, você ainda está aí?" já
    }

    // 1) Fez uma pergunta e a resposta não chegou: pergunta de novo, rápido
    if (REPERGUNTAR && !ditado && st.ultimaPergunta && !st.jaRepetiuPergunta && st.esperandoDesde) {
      const ouviuVoz = st.vozEm > st.esperandoDesde;
      const esperou = agora - st.esperandoDesde;
      // Depois da saudação a pessoa demora mais (ainda está entendendo quem ligou):
      // espera um pouco mais e não usa o atalho de 3s da "voz sem texto" (piloto: Alex)
      const naSaudacao = !st.historico.some((m) => m.role === "user");
      const limite = Math.min(naSaudacao ? REPERGUNTA_SAUDACAO_MS : REPERGUNTA_MS, st.silencioMs);
      const atalhoVoz = !naSaudacao && ouviuVoz && agora - st.vozEm >= REPERGUNTA_VOZ_MS;
      if (atalhoVoz || esperou >= limite) {
        st.jaRepetiuPergunta = true;
        // Nunca ouviu voz nenhuma na ligação: um "Alô?" curto em vez de repetir a saudação inteira
        const frase = !st.algumaVoz ? FRASE_ALO : `${FRASE_REPERGUNTA} ${st.ultimaPergunta}`;
        L(st, `[vigia] ${ouviuVoz ? "ouvi voz, mas não chegou texto" : `${(esperou / 1000).toFixed(1)}s sem resposta`} — ` +
          (!st.algumaVoz ? "ninguém falou até agora: \"Alô?\"" : "repetindo a pergunta"));
        st.vigiaOcupado = true;
        try { await falarAvulso(ws, st, frase, "repergunta"); }
        finally { st.vigiaOcupado = false; }
      }
      return;
    }

    // 2) Silêncio longo: resgate → resgate → despedida
    const mudoHa = agora - st.calado_desde;
    // Ninguém falou NADA na ligação (recado da operadora gravando, ou atendeu e largou):
    // depois do "Alô?" desliga logo e tenta de novo mais tarde
    if (!st.algumaVoz && st.jaRepetiuPergunta) {
      if (mudoHa < Math.min(SILENCIO_SEM_VOZ_MS, st.silencioMs)) return;
      st.motivoSemConversa = "Atendeu e ficou em silêncio";
      L(st, "[vigia] 🔇 ninguém falou nada na ligação — desligando (tenta de novo depois)");
      st.vigiaOcupado = true;
      try { await desligar(st, "silêncio total"); } finally { st.vigiaOcupado = false; }
      return;
    }
    if (mudoHa < st.silencioMs) return;
    const RESGATES = ["Alô, você ainda está aí?", "Se preferir, eu ligo em outro momento. Pode ser?"];
    st.vigiaOcupado = true;
    try {
      if (st.tentativasResgate < RESGATES.length) {
        const frase = RESGATES[st.tentativasResgate++];
        L(st, `[vigia] silêncio de ${Math.round(mudoHa / 1000)}s — resgate ${st.tentativasResgate}`);
        st.calado_desde = Date.now();
        await falarAvulso(ws, st, frase, "resgate");
      } else {
        L(st, "[vigia] sem resposta — despedindo e encerrando");
        const meu = await falarAvulso(ws, st, st.fraseDespedida, "despedida", { encerrar: true });
        for (let i = 0; i < 60 && st.falando && st.turno === meu; i++) await pausa(250);
        await pausa(1200);
        if (st.turno === meu) await desligar(st, "silêncio prolongado");
      }
    } finally { st.vigiaOcupado = false; }
  }, 500);

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.event === "start") {
      st.streamSid = msg.start?.streamSid || null;
      st.callSid = msg.start?.callSid || null;
      st.tag = (st.callSid || "").slice(-4) || "----";
      const p = msg.start?.customParameters || {};
      st.campanhaId = p.campanha_id || "";
      st.contatoId = p.contato_id || "";
      st.agenteId = p.agente_id || "";
      L(st, `[streams] início — callSid: ${st.callSid} | agente: ${st.agenteId || "(nenhum)"}`);
      iniciarConversa().catch((e) => LE(st, `[streams] erro ao iniciar: ${e?.message || e}`));
      return;
    }

    if (msg.event === "media") {
      st.pacotes++;
      if (st.pacotes % 1500 === 0) L(st, `[streams] áudio fluindo… ${st.pacotes} pacotes`);
      const audio = Buffer.from(msg.media.payload, "base64");
      if (st.dgPronto && st.dg && st.dg.readyState === 1) { try { st.dg.send(audio); } catch {} }
      else {
        if (!st.fila.length) st.filaDesde = Date.now();
        st.fila.push(audio);
        if (st.fila.length > 500) { st.fila.shift(); st.filaDesde += 20; }
      }
      return;
    }

    if (msg.event === "mark") {
      // Só vale a marca do turno atual (a Twilio devolve marcas velhas depois de um "clear")
      if ((msg.mark?.name || "") !== `t${st.turno}`) return;
      st.marcasPendentes = Math.max(0, st.marcasPendentes - 1);
      if (st.marcasPendentes === 0 && st.turnoPronto && st.falando) fimDaFala();
      return;
    }

    if (msg.event === "stop") {
      L(st, `[streams] fim do túnel — pacotes: ${st.pacotes}`);
      st.encerrando = true;   // a ligação acabou: nada de re-pergunta nem fala nova
      return;
    }
  });

  ws.on("close", async () => {
    st.fechado = true; st.encerrando = true; st.aguardandoAlo = false;
    st.turno++;                      // invalida o turno em andamento (para de pensar e de falar)
    registrarParcial(st);
    clearInterval(vigia); limparFlush();
    const duracao = Math.round((Date.now() - st.iniciadoEm) / 1000);
    const zap = st.whatsappConfirmado
      ? ` | whatsapp ✅ ${formatarTelefone(st.whatsappConfirmado)}`
      : st.numeroEntendido ? ` | whatsapp ❓ ${formatarTelefone(st.numeroEntendido)} (não confirmado)` : "";
    L(st, `[streams] túnel fechado (${duracao}s) | nome usado ${st.usosNome}x | número pedido ${st.pedidosNumero}x${zap}`);
    L(st, "═══════ TRANSCRIÇÃO DA LIGAÇÃO ═══════");
    st.transcricao.forEach((l, i) => L(st, `${String(i + 1).padStart(2, "0")} ${l}`));
    L(st, "══════════════════════════════════════");
    if (st.streamClaude) { try { st.streamClaude.abort(); } catch {} }
    if (st.dg) { try { st.dg.close(); } catch {} }

    const conversou = !st.caixaPostal && st.historico.some((m) => m.role === "user");
    const motivoSemConversa = conversou ? "" : (st.motivoSemConversa ||
      (st.triagem ? "Assistente de chamadas: não passou a ligação"
        : st.algumaVoz ? "Atendeu e desligou sem responder" : "Atendeu e ficou em silêncio"));
    await gravarLigacao({
      supabase: st.campanhaId ? st.supabase : null,
      callSid: st.callSid, campanhaId: st.campanhaId, contatoId: st.contatoId,
      transcricao: st.transcricao.join("\n"),
      houveConversa: conversou,
      duracao,
      extras: {
        whatsappConfirmado: st.whatsappConfirmado,
        whatsappNaoConfirmado: st.whatsappConfirmado ? "" : st.numeroEntendido,
        motivoSemConversa,
        naoLigarMais: st.naoLigarMais,
      },
      st,
    });
  });

  ws.on("error", (e) => { clearInterval(vigia); limparFlush(); LE(st, `[streams] erro: ${e?.message}`); });
});

server.listen(PORT, () => {
  console.log(`[VozIA] motor de voz ouvindo na porta ${PORT}`);
  avisarFaltando();
});
