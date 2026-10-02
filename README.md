# VozIA — Motor de Voz

Servidor que faz as ligações automáticas com voz humanizada do painel **VozIA**.

Ele conecta quatro peças:

- **Telnyx Call Control** — disca, manda o áudio (PCMU) e avisa atendimento/desligamento
- **Deepgram + ElevenLabs** — ouve e fala, no túnel `/ws-streams`
- **Claude (Anthropic)** — o cérebro da conversa
- **Supabase** — o mesmo banco do painel (lê campanhas/contatos, grava as ligações)
- Roda 24h na **Railway**

## Como funciona

1. O painel chama `POST /campanhas/iniciar` → o servidor liga via `POST https://api.telnyx.com/v2/calls`.
2. Quando a pessoa atende, a Telnyx abre o WebSocket em `/ws-streams` e manda os eventos `start` / `media` / `mark` / `stop`.
3. O que a pessoa fala vai pro Deepgram → Claude → ElevenLabs, e o áudio volta pelo mesmo túnel.
4. O fim da ligação chega em `POST /telnyx/webhook` (`call.hangup`). Transcrição, duração, resultado, sentimento e nota continuam no Supabase.

O id da ligação da Telnyx (`call_control_id`) é gravado na coluna já existente `twilio_call_sid`. O painel não precisa de migração de banco.

## Variáveis

- `TELNYX_API_KEY` — chave da API
- `TELNYX_CONNECTION_ID` — Call Control App / connection id (obrigatório pra discar)
- `TELNYX_FROM` — número de saída em E.164
- `PUBLIC_HOST` — domínio público da Railway, sem `https://`
- `VOICE_BACKEND_SECRET` — protege o discador e o webhook (`?k=`)

No app de voz da Telnyx, o webhook pode ficar em `https://SEU-DOMINIO/telnyx/webhook`. O motor também manda essa URL em cada ligação.

## Endpoints

- `GET /` — verificação de saúde
- `POST /campanhas/iniciar` — inicia o discador (Bearer `VOICE_BACKEND_SECRET`)
- `POST /telnyx/webhook` — eventos da Telnyx
- `WS /ws-streams` — a conversa em tempo real
