# Arquitectura del sistema
## Casos de uso
Una petición del cliente hace que el sistema procese los 5 casos de uso:

### UC1
Transacciones en USD con monto menor a 50.

### UC2
Monto de la máxima transacción en USD para cada banco.

### UC3
Transacciones en USD en el período 2022-09-06 al 2022-09-15 (período B), cuyo monto sea menor a un centésimo del promedio de monto para su formato en el período 2022-09-01 al 2022-09-05 (período A).

### UC4
Cuentas que cumplan con el patrón *scatter-gather* con una cuenta de separación y una cantidad mínima de cuentas intermedias igual a 5; en el período A.

### UC5
Cantidad de transacciones con formato de pago *Wire* o *ACH* en el período A, cuyo monto en USD sea menor a 1.

![](../diagrams/08use-cases-diagram.png){width=50%}

## Vista física
### Robustez

![](../diagrams/02robustness-diagram.png){width=90%}

![](../diagrams/02robustness-diagram-uc1.png){width=90%}

![](../diagrams/02robustness-diagram-uc2.png){width=90%}

![](../diagrams/02robustness-diagram-uc3.png){width=90%}

![](../diagrams/02robustness-diagram-uc4.png){width=90%}

![](../diagrams/02robustness-diagram-uc5.png){width=90%}

### Despliegue

![](../diagrams/04deployment-diagram.png){width=90%}

## Vista de procesos
### Actividades

![](../diagrams/05activity-diagram.png){width=90%}

### Secuencia

![](../diagrams/06secuence-diagram-case-1.png){width=90%}

![](../diagrams/06secuence-diagram-case-2.png){width=90%}

![](../diagrams/06secuence-diagram-case-3.png){width=90%}

![](../diagrams/06secuence-diagram-case-4.png){width=90%}

![](../diagrams/06secuence-diagram-case-5.png){width=90%}

## Vista de desarrollo
### Paquetes

![](../diagrams/07packages-diagram.png){width=50%}
