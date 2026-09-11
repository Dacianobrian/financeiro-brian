-- ============================================================================
--  FECHA O ACESSO ANÔNIMO NO SUPABASE
--  Projeto: cathhujdhocioybdpnvc
--
--  PROBLEMA
--  A chave "anon" fica publicada no HTML do site (isso é normal e esperado —
--  ela é pública por design). Quem protege os dados é o RLS. Só que 20 tabelas
--  estavam sem proteção real:
--    - 12 com RLS ligado mas com política liberando tudo (anon_full_access)
--    - 8 sem RLS nenhum
--  Na prática, qualquer pessoa que abrisse o código-fonte do site conseguia
--  LER e APAGAR o histórico financeiro, a carteira de FIIs e as tabelas de
--  leads/conversas do CRM.
--
--  O QUE ESTE SCRIPT FAZ
--  1. Liga o RLS onde faltava
--  2. Apaga as políticas permissivas
--  3. Cria uma política única: só sessão autenticada
--  4. Revoga na marra os privilégios da role anon (segunda camada de defesa)
--
--  NÃO QUEBRA
--  - Automações server-side que usem a chave service_role (ela ignora o RLS)
--  - O app financeiro atual (movimentos/tags/cartoes/etc já estavam corretos
--    e não são tocados aqui)
--
--  PODE QUEBRAR
--  - Qualquer app seu que leia estas tabelas com a chave anon SEM fazer login.
--    Se algo parar de funcionar, o rollback está no arquivo 02.
--
--  COMO RODAR
--  Supabase → SQL Editor → cole tudo → Run
-- ============================================================================

do $$
declare
  t text;
  p record;
  alvos text[] := array[
    -- sistema financeiro antigo (Flask)
    'transactions','accounts','fixed_costs','goals','fii_portfolio',
    'installments','subscriptions','planned_entries','planned_investments','budgets',
    -- CRM imobiliário
    'leads','conversations','crm_settings','daily_metrics',
    -- produtividade
    'ideas','habits','habit_checks','agenda_events','tasks','shopping_items'
  ];
begin
  foreach t in array alvos loop
    execute format('alter table public.%I enable row level security', t);

    for p in select policyname from pg_policies
             where schemaname = 'public' and tablename = t loop
      execute format('drop policy %I on public.%I', p.policyname, t);
    end loop;

    execute format(
      'create policy "somente_autenticado" on public.%I
         for all to authenticated using (true) with check (true)', t);

    execute format('revoke all on public.%I from anon', t);
    execute format('grant select, insert, update, delete on public.%I to authenticated', t);

    raise notice 'protegida: %', t;
  end loop;
end $$;

-- Conferência: depois de rodar, esta consulta não deve trazer nenhuma linha
-- com role "anon" nem com qual = "true" para as tabelas acima.
select tablename, policyname, roles::text, cmd, qual
from pg_policies
where schemaname = 'public'
order by tablename;
