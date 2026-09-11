# Segurança do sistema — o que foi feito e o que falta

Levantamento de 11/09/2026.

## Como o sistema funciona (contexto)

O site é estático (GitHub Pages, pasta `docs/`) e fala direto com o Supabase pelo
navegador. Não existe servidor no meio. Isso significa que a chave `anon` fica
**publicada no código-fonte da página** — isso é normal e esperado, é assim que o
Supabase foi desenhado.

A consequência é que **toda a proteção dos dados depende do RLS** (Row Level
Security) no banco. Se uma tabela não tem RLS correto, ela está aberta para
qualquer pessoa na internet que abra o código-fonte do site.

## ✅ Feito — já está no código

| Item | O que resolve |
|---|---|
| **CSP** em todas as 10 páginas | Se algum script malicioso entrar na página, ele não consegue mandar seus dados para fora: `connect-src` só permite falar com o seu Supabase. Também bloqueia plugins, `<base>` e envio de formulário para fora. |
| **`esc()` no `index.html`** | O texto que você digita (descrição de lançamento, nome de tag) passava cru para o HTML. Agora é escapado, como já era nas telas novas. |
| **`referrer: no-referrer`** | Para de vazar a URL das suas páginas para os sites externos (Google Fonts, CDN). |
| **`old.html` e `index.html.bak` removidos** | Eram 774 KB de código antigo publicados junto com o site. Continuam no histórico do git. |
| **Chave tirada do workflow** | O `supabase-keep-alive.yml` tinha a chave anon escrita direto no arquivo. Agora vem do secret `SUPABASE_ANON_KEY`, e o ping usa `movimentos` (protegida por RLS) em vez de `accounts`. |

Verificado com Chromium headless: as 10 páginas carregam com **zero violações de
CSP**.

## 🔴 Falta — precisa de você

### 1. Rodar o SQL (o mais importante)

**20 tabelas estão abertas para qualquer pessoa ler e apagar**, inclusive
`transactions` (711 lançamentos), `fii_portfolio`, e as tabelas `leads` /
`conversations` do CRM.

```
Supabase → SQL Editor → cole o 01-fechar-acesso-anonimo.sql → Run
```

Não quebra automações que usem a chave `service_role` (ela ignora RLS). Pode
quebrar algum app seu que leia essas tabelas **sem login**. Se isso acontecer,
o `02-rollback.sql` desfaz — de preferência editando a lista para reverter só a
tabela que quebrou.

Depois de rodar, confira: nenhuma linha do resultado final pode ter role `anon`.

### 2. Configurações grátis no painel do Supabase

- **Authentication → Providers → Email**: desligar "Enable sign ups". Hoje
  qualquer pessoa pode criar conta no seu app. Existe só 1 conta (a sua), então
  desligar não te atrapalha.
- **Authentication → Policies → Password**: ligar "Leaked password protection"
  (checa a senha contra vazamentos conhecidos) e exigir mínimo de 10 caracteres.
- **Authentication → MFA**: ativar 2FA na sua conta.

### 3. Dois pontos de higiene

- `backups/backup_2026-09-11.md` tem seus lançamentos reais (valores, aluguel,
  comissões) num **repositório público**. Vale mover para fora do git.
- O sistema Flask antigo (`app.py`, `database.py`, `templates/`, `Procfile`,
  `railway.toml`) não é mais usado. Se ainda estiver de pé no Railway com a chave
  no ambiente, é uma porta aberta a menos se for desligado.

### 4. SRI no script do Supabase (opcional)

Hoje as páginas carregam `supabase-js` do CDN jsdelivr sem verificação. Se o CDN
for comprometido, o script injetado roda com sua sessão. O SRI trava o arquivo
num hash específico. Não consegui gerar o hash aqui (rede bloqueada); na sua
máquina:

```bash
curl -sL https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.js \
  | openssl dgst -sha384 -binary | openssl base64 -A
```

Depois é só acrescentar nas 10 páginas:

```html
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.js"
        integrity="sha384-COLE_O_HASH_AQUI" crossorigin="anonymous"></script>
```
