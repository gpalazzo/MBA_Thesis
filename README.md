## ToDos
### Otávio


## Dúvidas
1. por que em alguns casos (100 e poucos) a data do pedido é posterior a data de faturamento?

2. na aba "Qualificação Clientes - CRM", os dados são estáticos como uma foto do momento mais atual, mas como linkamos isso com uma data?
    - na aba "Origem de Receita e Frota - CRM", a coluna "DTALTERADO" tem uma data de alteração. faz sentido considerar essa data como universal para as alterações na base de CRM?

3. na aba "Origem de Receita e Frota - CRM" tem cliente com 2 áreas de mesmo tamanho e mesmo valor na coluna "SEQPROPRIEDADE" que deveria ser um id da propriedade. faz sentido isso?
    - exemplo prático: id_cliente = 4988

4. dados pluviométricos
    - a latitude e longitude é da estação ou da cidade? se for da estação, você consegue da cidade?
        - talvez tenha no IBGE
        - eu achei uma solução aqui pela programação, mas nunca usei então não sei se funciona

5. Produtividade e Produção Laranja SP
    - não tem um formato mais tabular desses dados?
        - coloquei um exemplo na outra aba
    - coloquei data em amarelo na planilha, o que ela significa?

6. Área de Produção de Laranja por Cliente - Itaeté Jales
    - coluna "SEQUENCIAL" é o SeqPessoa da base de dados? SIM!
    - qual a diferença desse dado de área para os dados na planilha de CRM e BI na aba "Origem de Receita e Frota - CRM" coluna "O"?
    * usar o dessa planilha e não da Origem de Receita e Frota - CRM

7. Faturamento Citrus Jales
    - qual a diferença disso para os dados da planilha de CRM e BI da aba "Análise Fin - BI" na coluna "U"?
    - usar a "Análise Fin - BI" ao invés dessa

8. Planilha "Frota Clientes - Itaeté Jales"
    - pode considerar que o ano do trator é o ano que adquiriu
    - SEQUENCIAL é o SeqPessoa
    - features: quantidade tratores, idade média da frota, ...
