/* ══════════════════════════════════════════════════════════════
   CICLO DA FATURA
   Onde a fatura de um mês começa e termina, quando vence e quanto soma.

   Mora em arquivo próprio, e não dentro de cada página, porque a tela de
   cartões e a da fatura precisam da MESMA resposta: duas cópias divergiriam
   na primeira mudança de regra, e aí o valor na frente do cartão deixaria de
   bater com o da fatura que ele abre.
   ══════════════════════════════════════════════════════════════ */
const Fatura = (() => {

  const ultimoDia = (y, m) => new Date(y, m + 1, 0).getDate();

  /* Dia 31 num mês de 30 cai no último dia: o cartão fecha assim mesmo. */
  const noMes = (y, m, d) => new Date(y, m, Math.min(d, ultimoDia(y, m)));

  /* Vence depois de fechar: no mesmo mês quando o dia de vencimento vem
     depois do de fechamento, no mês seguinte quando vem antes — fechar dia 28
     e vencer dia 5 é vencer em novembro, não no mesmo outubro. */
  function vencimento(c, ano, mes, fecha){
    const vence = c && c.dia_vencimento ? +c.dia_vencimento : null;
    if (!vence) return null;
    return noMes(ano, fecha === null || vence > fecha ? mes : mes + 1, vence);
  }

  /* A fatura de (ano, mes) fecha no dia de fechamento desse mês e cobre do dia
     seguinte ao fechamento anterior. Sem dia de fechamento cadastrado, cai no
     mês corrido — melhor que não mostrar nada. */
  function ciclo(c, ano, mes){
    const fecha = c && c.dia_fechamento ? +c.dia_fechamento : null;
    let ini;
    if (fecha === null){
      ini = new Date(ano, mes, 1);
    } else {
      ini = noMes(ano, mes - 1, fecha);
      ini.setDate(ini.getDate() + 1);
    }
    ini.setHours(0, 0, 0, 0);
    const fim = fecha === null ? new Date(ano, mes + 1, 0) : noMes(ano, mes, fecha);
    /* O fim é o dia inteiro. Os lançamentos ficam guardados ao meio-dia, então
       um fim à meia-noite jogaria a compra do próprio dia de fechamento para a
       fatura seguinte. */
    fim.setHours(23, 59, 59, 999);
    return { ini, fim, vence: vencimento(c, ano, mes, fecha) };
  }

  function lancamentos(c, movs, ano, mes){
    const { ini, fim } = ciclo(c, ano, mes);
    return movs
      .filter(mv => String(mv.cartao_id || '') === String(c.id) && mv.data >= ini && mv.data <= fim)
      .sort((a, b) => a.data - b.data);
  }

  /* Estorno é entrada lançada no cartão: abate da fatura em vez de somar. */
  function total(c, movs, ano, mes){
    return lancamentos(c, movs, ano, mes)
      .reduce((a, mv) => a + (mv.tipo === 'entrada' ? -mv.valor : mv.valor), 0);
  }

  /* 'aberta' enquanto ainda entra gasto, 'fechada' à espera do vencimento,
     'vencida' passado ele. Sem dia de vencimento a fatura para em 'fechada':
     não dá para dizer que venceu sem saber quando vencia. */
  function situacao(c, ano, mes, hoje = new Date()){
    const { fim, vence } = ciclo(c, ano, mes);
    if (hoje <= fim) return 'aberta';
    if (!vence) return 'fechada';
    const prazo = new Date(vence);
    prazo.setHours(23, 59, 59, 999);
    return hoje <= prazo ? 'fechada' : 'vencida';
  }

  return { ciclo, lancamentos, total, situacao };
})();
