# VozIA — Motor de Voz

Servidor que faz as ligações automáticas com voz humanizada do painel **VozIA**.

Ele conecta quatro peças:

- **Twilio Conversation Relay** — faz a ligação, ouve (Deepgram) e fala (ElevenLabs)
- **Claude (Anthropic)** — o cérebro da conversa
- **Supabase** — o mesmo banco do painel (lê campanhas/contatos, grava as ligações)
- Roda 24h na **Railway**

## Como funciona

1. O painel chama `POST /campanhas/iniciar` → o servidor liga para os contatos da fila (via Twilio).
2. Quando a pessoa atende, a Twilio busca o TwiML em `/twiml` e abre um WebSocket em `/ws`.
3. Em `/ws`, o que a pessoa fala vira texto → vai pro Claude → a resposta volta em voz.
4. No fim da ligação, a transcrição, a duração, o resultado, o sentimento e a nota são salvos no Supabase.

## Rodando

A Railway roda `npm start` automaticamente. Todas as configurações vêm de variáveis
de ambiente — veja o arquivo `.env.example`. **Nenhuma chave fica no código.**

## Endpoints

- `GET /` — verificação de saúde (deve responder "online")
- `ALL /twiml` — TwiML para a Conversation Relay
- `POST /campanhas/iniciar` — inicia o discador (protegido por `VOICE_BACKEND_SECRET`)
- `WS /ws` — a conversa em tempo real
