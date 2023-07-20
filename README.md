## ToDos
1. ver a diferença em dias entre data de pedido e faturamento para criar uma proxy para os pedidos sem faturamento
    - como a diferença da janela é considerando o faturamento, se não fizer esse ajuste, os pedidos com label 0 não tem data inferior para considerar os dados

## Dúvidas
1. por que em alguns casos (100 e poucos) a data do pedido é posterior a data de faturamento?
2. na aba "Qualificação Clientes - CRM", os dados são estáticos como uma foto do momento mais atual, mas como linkamos isso com uma data?
    - na aba "Origem de Receita e Frota - CRM", a coluna "DTALTERADO" tem uma data de alteração. faz sentido considerar essa data como universal para as alterações na base de CRM?
3. na aba "Origem de Receita e Frota - CRM" tem cliente com 2 áreas de mesmo tamanho e mesmo valor na coluna "SEQPROPRIEDADE" que deveria ser um id da propriedade. faz sentido isso?
    - exemplo prático: id_cliente = 4988
