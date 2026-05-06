---
geometry: top=2.54cm, bottom=2.54cm, left=1.9cm, right=1.9cm
fontsize: 12pt
linestretch: 0.98
lang: es
header-includes:
  - \usepackage{float}
  - \makeatletter
  - \def\fps@figure{H}
  - |
    \renewcommand\paragraph{\@startsection{paragraph}{4}{\z@}%
      {-2.5ex\@plus -1ex \@minus -.2ex}%
      {0.5ex \@plus .2ex}%
      {\normalfont\normalsize\bfseries}}
  - \makeatother
  - \usepackage{svg}
  - \usepackage{xcolor}
  - \usepackage{listings}
  - |
    \lstset{
      language=C,
      breaklines=true,
      breakatwhitespace=true,
      basicstyle=\ttfamily\small,
      columns=fullflexible,
      numbers=left,
      numberstyle=\tiny\color{gray},
      stepnumber=1,
      numbersep=8pt,
      keywordstyle=\color{blue}\bfseries,
      commentstyle=\color{gray},
      stringstyle=\color{red!70!black},
      showstringspaces=false
    }
---

\begin{titlepage}
    \centering
    \vspace*{5cm}

    \includegraphics[width=0.4\textwidth]{img/logo-fiuba.png}\\[1cm]

    {\Huge \textbf{Money Laundering Analysis}}\\[0.2cm]

    {\large \textbf{Sistemas Distribuidos}}\\[0.8cm]

    \begin{tabular}{lll}
        108397 - & \textbf{Alejo Ordoñez} \\[-2pt]
        108397 - & \textbf{Minervino Lorenzo} \\[-2pt]
        108397 - & \textbf{Valsagna Federico} \\[-2pt]
    \end{tabular}

    \vfill

    {\large 1c2026}
\end{titlepage}

\newpage
\tableofcontents
\newpage
