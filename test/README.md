# Test
## Accounts repetido
Para hacer el procesamiento las cuentas que se necesitan varían según el dataset de transacciones, o sea voy a necesitar el subconjunto tal que me de, en particular, la data para todos los bancos del UC2. Este análisis está hecho y así se obtuvieron los datasets `transactions100.csv` y `accounts42.csv`, pero acá directamente usamos el dataset (chiquito) entero `LI-Small_accounts.csv` para las cuentas, porque los samples del dataset de transacciones van a ser random por cada ejecución de `gen_input_output.py`.
