-- ============================================================================
--  ROLLBACK — desfaz o 01-fechar-acesso-anonimo.sql
--
--  Use SÓ se algum app seu parar de funcionar depois de fechar o acesso.
--  Isso devolve o acesso anônimo (inseguro). O ideal é rodar o rollback apenas
--  na tabela específica que quebrou, não em todas — edite a lista "alvos"
--  abaixo deixando só o nome da tabela problemática.
-- ============================================================================

do $$
declare
  t text;
  p record;
  alvos text[] := array[
    'transactions','accounts','fixed_costs','goals','fii_portfolio',
    'installments','subscriptions','planned_entries','planned_investments','budgets',
    'leads','conversations','crm_settings','daily_metrics',
    'ideas','habits','habit_checks','agenda_events','tasks','shopping_items'
  ];
begin
  foreach t in array alvos loop
    for p in select policyname from pg_policies
             where schemaname = 'public' and tablename = t loop
      execute format('drop policy %I on public.%I', p.policyname, t);
    end loop;

    execute format('grant select, insert, update, delete on public.%I to anon', t);
    execute format(
      'create policy "anon_full_access" on public.%I
         for all to anon using (true) with check (true)', t);

    raise notice 'revertida (INSEGURA): %', t;
  end loop;
end $$;
