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

## 🔴 Falta — o que depende de você

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

**Feito em parte.** O arquivo `backups/backup_2026-09-11.md` saiu do repositório e
a pasta `backups/` entrou no `.gitignore`, então nunca mais volta. O conteúdo foi
devolvido para você guardar fora do GitHub.

**O que continua exposto:** o arquivo ainda está alcançável no commit `f38207c`.
Apagar não apaga o passado.

**Por que eu não reescrevi o histórico:** duas razões.

A primeira é prática — o sistema de permissões deste ambiente bloqueia reescrita
de histórico e `push --force`.

A segunda é mais importante, e mudaria a recomendação mesmo sem o bloqueio:
**reescrever o histórico e dar force-push não garante que o dado sai do GitHub.**
Os commits antigos deixam de aparecer na listagem, mas continuam acessíveis por
URL direta com o SHA (`github.com/.../commit/f38207c`) até o GitHub fazer coleta
de lixo — o que não tem prazo e, na prática, só acontece se você abrir um chamado
no Suporte pedindo. Ou seja: o trabalho todo, e o dado possivelmente continua lá.

**As opções reais, então:**

| Opção | Resolve de verdade? | Custo |
|---|---|---|
| Reescrever histórico + force-push | Parcial — some da listagem, pode continuar acessível por SHA | Quebra clones locais |
| Reescrever + abrir chamado no Suporte do GitHub pedindo a coleta de lixo | Sim | Trabalhoso, depende do Suporte |
| Apagar o repositório e criar de novo, só com os arquivos atuais | Sim, definitivo | Perde todo o histórico de commits |
| Tornar o repositório privado | Sim | Derruba o site (Pages gratuito exige repositório público) |
| Não fazer nada além do que já foi feito | Não | Zero |

**Sugestão honesta:** o que vazou são seus gastos pessoais — aluguel, comissões,
consórcio. Não é senha nem chave, então não há nada para "rotacionar" e o estrago
não cresce com o tempo. O repositório tem **0 forks**, ou seja, ninguém copiou.
Se isso te incomoda de verdade, a opção limpa é apagar e recriar o repositório.
Se é aceitável, o mais importante já está feito: não acontece de novo.

Os comandos, se você optar por reescrever:

```bash
git clone https://github.com/Dacianobrian/financeiro-brian.git
cd financeiro-brian
git filter-branch --index-filter \
  'git rm --cached --ignore-unmatch backups/backup_2026-09-11.md' \
  --prune-empty -- --all
git push --force --all
git push --force --tags
```

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
