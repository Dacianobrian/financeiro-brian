# Segurança do sistema — o que foi feito e o que falta

Levantamento e correção de 11/09/2026.

## Como o sistema funciona (contexto)

O site é estático (GitHub Pages, pasta `docs/`) e fala direto com o Supabase pelo
navegador. Não existe servidor no meio. Isso significa que a chave `anon` fica
**publicada no código-fonte da página** — isso é normal e esperado, é assim que o
Supabase foi desenhado.

A consequência é que **toda a proteção dos dados depende do RLS** (Row Level
Security) no banco. Se uma tabela não tem RLS correto, ela está aberta para
qualquer pessoa na internet que abra o código-fonte do site.

## O que estava errado

Simulando um visitante anônimo dentro do próprio banco, era isso que qualquer
pessoa enxergava — e podia apagar:

| Tabela | Linhas expostas |
|---|---|
| `transactions` | **711** |
| `installments` | 29 |
| `fixed_costs` | 12 |
| `crm_settings` | 11 |
| `fii_portfolio` | 11 |
| `budgets` | 8 |
| `ideas` | 4 |
| `movimentos` (app atual) | 0 ✅ |

Ao todo **20 tabelas**: 12 tinham RLS ligado mas com uma política `anon_full_access`
que liberava tudo, e 8 não tinham RLS nenhum. As 5 tabelas do app financeiro atual
(`movimentos`, `tags`, `cartoes`, `movimento_tags`, `previsao_diario`) já estavam
corretas — por isso o zero.

## ✅ Feito — banco

Aplicado em 11/09/2026 (`01-fechar-acesso-anonimo.sql`).

- RLS ligado nas 8 tabelas que estavam sem
- Políticas permissivas removidas, substituídas por `somente_autenticado`
- Privilégios da role `anon` revogados nas 20 tabelas (segunda camada: mesmo que
  uma política frouxa volte por engano, `anon` não alcança a tabela)
- `search_path` fixado nas 7 funções do banco

**Verificado depois de aplicar:**

- Como `anon`: `ERROR: 42501: permission denied for table transactions` ✅
- Como usuário logado: 711 lançamentos, 29 parcelamentos, tudo no lugar ✅
- Painel do Supabase: o alerta crítico de RLS desapareceu ✅

Automações que usem a chave `service_role` não foram afetadas — essa chave ignora
o RLS por definição.

### Sobre as 5 tabelas do app atual

Numa auditoria elas aparecem como "anon ainda tem grant". **Isso está certo e é
proposital**: elas usam a política `auth.uid() = user_id`, que é o padrão do
Supabase. O `anon` tem permissão na tabela, mas como não tem usuário logado, o
RLS devolve lista vazia. Foi testado: `anon` vê 0 linhas. É também o que faz o
ping do keep-alive responder 200 sem vazar nada.

## ✅ Feito — site

| Item | O que resolve |
|---|---|
| **CSP** em todas as 10 páginas | Se algum script malicioso entrar na página, ele não consegue mandar seus dados para fora: `connect-src` só permite falar com o seu Supabase. Também bloqueia plugins, `<base>` e envio de formulário para fora. |
| **`esc()` no `index.html`** | O texto que você digita (descrição de lançamento, nome de tag) passava cru para o HTML. Agora é escapado, como já era nas telas novas. |
| **`referrer: no-referrer`** | Para de vazar a URL das suas páginas para os sites externos (Google Fonts, CDN). |
| **`old.html` e `index.html.bak` removidos** | Eram 774 KB de código antigo publicados junto com o site. Continuam no histórico do git. |
| **Chave tirada do workflow** | O `supabase-keep-alive.yml` tinha a chave anon escrita direto no arquivo. Agora vem do secret `SUPABASE_ANON_KEY`, e o ping usa `movimentos` (protegida por RLS). |

Verificado com Chromium headless: as 10 páginas carregam com **zero violações de
CSP**.

## 🔴 Falta — 3 coisas que dependem de você

### 1. Dois cliques no painel do Supabase

São as únicas coisas que ficam fora do alcance de script — o Supabase só permite
mudar pelo painel:

- **Authentication → Providers → Email**: desligar **"Enable sign ups"**. Hoje
  qualquer pessoa pode criar conta no seu app. Existe só 1 conta (a sua), então
  desligar não te atrapalha em nada.
- **Authentication → Policies**: ligar **"Leaked password protection"**. Checa sua
  senha contra bases de vazamentos conhecidos. É o último aviso que sobrou no
  painel.

### 2. O secret do GitHub

O workflow de keep-alive agora lê a chave de um secret em vez de tê-la escrita no
arquivo. Para ele voltar a rodar:

```
GitHub → repositório → Settings → Secrets and variables → Actions
→ New repository secret
   Nome:  SUPABASE_ANON_KEY
   Valor: a chave anon (está em Supabase → Settings → API → anon public)
```

Enquanto o secret não existir, o ping roda com a chave vazia e falha. Não afeta o
site, só o aviso de "projeto inativo" do Supabase.

### 3. O backup com seus valores reais

`backups/backup_2026-09-11.md` tem seus lançamentos de verdade — aluguel,
comissões, consórcios, plano de saúde — num repositório **público**.

Apagar o arquivo não basta: ele continua no histórico do git, e qualquer pessoa
consegue recuperá-lo. Limpar de verdade exige reescrever o histórico do
repositório, o que é uma operação delicada. As opções, da mais simples à mais
completa:

1. **Tornar o repositório privado** (Settings → General → Change visibility).
   Resolve tudo de uma vez, mas derruba o GitHub Pages no plano gratuito —
   o site sairia do ar.
2. **Reescrever o histórico** para remover só esse arquivo. O site continua no ar
   e o dado some. Dá para fazer, mas precisa da sua autorização explícita.
3. **Deixar como está** e daqui pra frente guardar backup fora do git.

## Opcional — SRI no script do Supabase

Hoje as páginas carregam `supabase-js` do CDN jsdelivr sem verificação. Se o CDN
for comprometido, o script injetado roda com a sua sessão. O SRI trava o arquivo
num hash específico. Não consegui gerar o hash aqui (a rede do ambiente bloqueia
o jsdelivr); na sua máquina:

```bash
curl -sL https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.js \
  | openssl dgst -sha384 -binary | openssl base64 -A
```

Depois é só acrescentar nas 10 páginas:

```html
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.js"
        integrity="sha384-COLE_O_HASH_AQUI" crossorigin="anonymous"></script>
```
