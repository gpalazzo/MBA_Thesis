## Labeling

### Filtros
    - considera apenas clientes onde a origem da receita é laranja
    - remove linhas quando data do pedido e do faturamento são nulas

### Regras
    - a data de faturamento é o target date
    - o horizonte de dias para criação das features é uma lookback window a partir da data de faturamento
        - ver parâmetros para definir o tamanho da lookback window

### Ajustes
    - linhas com data de faturamento (tendo ou não data de pedido) recebem a label "compra"
    - linhas com data de pedido, mas sem data de faturamento recebem a label "não_compra"
    - para linhas com data de pedido, mas sem data de faturamento, é necessário imputar uma data de faturamento dado a regra de lookback window usar o faturamento
        - para resolver isso, calcula o tempo mediano em dias (e não médio) passados entre data de pedido e faturamento dos últimos 3 anos, e soma esse valor na data do pedido, gerando uma data de faturamento aproximada

### Caveats
    - o input de data de faturamento poderia ser mais restritivo considerando apenas os perfis parecidos com o perfil e data que precisa ser imputado
