- ingestão de dados do raw para o primary segue o fluxo padrão de não alteração do conteúdo do dado, ou seja, podem ser feitas modificações no dado, mas a estrutura inicial se mantém preservada

- após isso, nas features:
1. input de nulos desconsidera o nível de cliente e trabalha apenas com as datas
2. sumarização das features considerando tanto o cliente quanto a data alvo
    - evitar misturar os cenários/especificidades de cada cliente e evitar data leakage
3. construção da master table sem considerar o nível de cliente
