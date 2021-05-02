import pandas as pd
import numpy as np

def total_casos_novos_municipio_infeccao_ano_14(df):
    '''
    | Importância do indicador: Conhecer a ocorrência de casos de LV, a distribuição espacial e temporal e
    | a tendência;
    |
    |Método de cálculo: Número total de casos novos de LV por local provável de infecção (UF, município, região administrativa ou localidade) no ano de notificação
    | input:
    | - df (dataframe com dados históricos)
    | - local (UF, município, região administrativa ou localidade)
    | - ano (2007 a 2019)
    | output:
    | - dataframe com Número total de casos novos de LV por local provável de infecção.
    '''
    casos_novos = df.loc[
        (df.ENTRADA == 1) & 
        (~df.CO_MN_INF.isnull()) & 
        (~df.CO_MN_INF.astype('string').str.contains('0000')), 
        :
    ].copy()
    casos_novos.CO_MN_INF = casos_novos.CO_MN_INF.astype(int)
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = casos_novos['ANO'].unique()
    mun.sort()
    anos.sort()   
    
    casos_novos_por_ano = pd.DataFrame(index=mun)
    for ano in anos:
        casos_novos_por_ano = pd.concat([
            casos_novos_por_ano,
            casos_novos.loc[casos_novos.ANO == ano, :].groupby('CO_MN_INF')['CO_MN_INF'].count().rename(ano)
        ], axis=1)
    return casos_novos_por_ano.fillna(0)

   
def total_casos_novos_municipio_residencia_ano_14(df):
    '''
    | Importância do indicador: Conhecer a ocorrência de casos de LV, a distribuição espacial e temporal e
    | a tendência;
    |
    |Método de cálculo: Número total de casos novos de LV por local de residencia (município) 
    | no ano de notificação
    | input:
    | - df (dataframe com dados históricos)
    | - local (UF, município, região administrativa ou localidade)
    | - ano (2007 a 2019)
    | output:
    | - dataframe com Número total de casos novos de LV por local provável de residência.
    '''
    casos_novos = df.loc[
        (df.ENTRADA == 1) & 
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    casos_novos.CO_MN_RESI = casos_novos.CO_MN_RESI.astype(int)
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = casos_novos['ANO'].unique()
    mun.sort()
    anos.sort()   
    
    casos_novos_por_ano = pd.DataFrame(index=mun)
    for ano in anos:
        casos_novos_por_ano = pd.concat([
            casos_novos_por_ano,
            casos_novos.loc[casos_novos.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
    return casos_novos_por_ano.fillna(0)

def taxa_geral_incidencia_municipio_infeccao_ano_15(df, df_pop):
    '''
    | Importância do indicador:
    | - Está relacionado à exposição de indivíduos à picada de fêmeas de flebotomíneos infectadas com
    | protozoários do gênero Leishmania;
    | - Identificar e monitorar no tempo o risco de ocorrência de casos de LV em determinada
    | população;
    | - Permite analisar as variações populacionais, geográficas e temporais na frequência de casos
    | confirmados de LV, como parte do conjunto de ações de vigilância epidemiológica e ambiental
    | da doença;
    | - Contribui para a avaliação e orientação das medidas de controle vetorial de flebotomíneos;
    | - Subsidia processos de planejamento, gestão e avaliação de políticas e ações de saúde
    | direcionadas ao controle da LV;
    | 
    | Limitações do indicador: Alguns casos do numerador não estarão contidos no denominador (casos
    | alóctones). 
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV por local provável de infecção (município) 
    | no ano de notificação dividido por População total do município no
    | ano de notificação x 100.000
    '''
    taxa = total_casos_novos_municipio_infeccao_ano_14(df)
    for idx in taxa.index:
        try:
            taxa.loc[idx] = taxa.loc[idx] / df_pop.loc[idx] * 100000
        except:
            print(idx)
    return taxa.fillna(0)
   
def proporcao_casos_confirmados_criterio_laboratorial_16(df):
    """
    | Importância do indicador:
    | - Permite avaliar de forma indireta a assistência ao paciente.
    | - Depende das condições técnico-operacionais do sistema de vigilância epidemiológica, em cada
    | área geográfica, para detectar, notificar, investigar e realizar testes laboratoriais específicos para
    | a confirmação diagnóstica.
    | - O maior percentual de casos confirmados por critério laboratorial está relacionado com uma boa
    | capacidade operacional do serviço de laboratório.
    | - Permite melhorar a especificidade do sistema de vigilância.
    | - Provê bases para planejamento do programa de controle da doença (insumos laboratoriais,
    | capacitação de profissionais nas atividades de laboratório).
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV confirmados por critério laboratorial,
    | agrupados por local de residência (município) no ano de notificação dividido 
    | por número total de casos novos de LV, por local de residência (município) 
    | no ano de notificação x 100
    """
    data = df.loc[
        (df.ENTRADA == 1) & 
        (df.CRITERIO == 1) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    
    casos = total_casos_novos_municipio_residencia_ano_14(df) 
    casos_laboratoriais_por_ano = pd.DataFrame(index=mun)
    for ano in anos:
        casos_laboratoriais_por_ano = pd.concat([
            casos_laboratoriais_por_ano,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
            
    for idx in casos_laboratoriais_por_ano.index:
        try:
            casos_laboratoriais_por_ano.loc[idx] = casos_laboratoriais_por_ano.loc[idx] / casos.loc[idx] * 100
        except:
            print(idx)
        
    return casos_laboratoriais_por_ano.fillna(0)

def proporcao_casos_menor_5_anos_17(df):
    """
    | Importância do indicador:
    | -Analisar variações populacionais, geográficas e temporais e subsidiar
    | processos de planejamento, gestão e avaliação de políticas e ações de saúde direcionadas ao controle
    | da LV.
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV em < 5 anos agrupados por local de infecção (município) 
    | no ano de notificação dividido por número total de casos novos de LV por local de infecção (município) 
    | no ano de notificação x 100
    """
    data = df.loc[
        (df.ENTRADA == 1) & 
        (df.IDADE < 5) &
        (~df.CO_MN_INF.isnull()) & 
        (~df.CO_MN_INF.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_INF = data.CO_MN_INF.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort()     
    casos = total_casos_novos_municipio_infeccao_ano_14(df)    
    menor_5_anos = pd.DataFrame(index=mun)
    
    for ano in anos:
        menor_5_anos = pd.concat([
            menor_5_anos,
            data.loc[data.ANO == ano, :].groupby('CO_MN_INF')['CO_MN_INF'].count().rename(ano)
        ], axis=1)
     
    for idx in menor_5_anos.index:
        try:
            menor_5_anos.loc[idx] = menor_5_anos.loc[idx] / casos.loc[idx] * 100
        except:
            print(idx)
        
    return menor_5_anos.fillna(0)

def proporcao_casos_50_anos_mais_18(df):
    """
    | Importância do indicador:
    | -Analisar variações populacionais, geográficas e temporais e subsidiar
    | processos de planejamento, gestão e avaliação de políticas e ações de saúde direcionadas ao controle
    | da LV.
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV em >= 50 anos agrupados por local de infecção (município) 
    | no ano de notificação dividido por número total de casos novos de LV por local de infecção (município) 
    | no ano de notificação x 100
    """
    data = df.loc[
        (df.ENTRADA == 1) & 
        (df.IDADE >= 50) &
        (~df.CO_MN_INF.isnull()) & 
        (~df.CO_MN_INF.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_INF = data.CO_MN_INF.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    casos = total_casos_novos_municipio_infeccao_ano_14(df)
    cinquenta_anos_mais = pd.DataFrame(index=mun)
    for ano in anos:
        cinquenta_anos_mais = pd.concat([
            cinquenta_anos_mais,
            data.loc[data.ANO == ano, :].groupby('CO_MN_INF')['CO_MN_INF'].count().rename(ano)
        ], axis=1)
    
    for idx in cinquenta_anos_mais.index:
        try:
            cinquenta_anos_mais.loc[idx] = cinquenta_anos_mais.loc[idx] / casos.loc[idx] * 100
        except:
            print(idx)
        
    return cinquenta_anos_mais.fillna(0)

def proporcao_casos_hiv_19(df):
    """
    | Importância do indicador:
    | -Analisar variações populacionais, geográficas e temporais e subsidiar
    | processos de planejamento, gestão e avaliação de políticas e ações de saúde direcionadas ao controle
    | da LV.
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV em coinfectados por HIV agrupados por local de infecção (município) 
    | no ano de notificação dividido por número total de casos novos de LV por local de infecção (município) 
    | no ano de notificação x 100
    """
    data = df.loc[
        (df.ENTRADA == 1) & 
        (df.HIV == 1) &
        (~df.CO_MN_INF.isnull()) & 
        (~df.CO_MN_INF.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_INF = data.CO_MN_INF.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    casos = total_casos_novos_municipio_infeccao_ano_14(df)
    hiv = pd.DataFrame(index=mun)

    for ano in anos:
        hiv = pd.concat([
            hiv,
            data.loc[data.ANO == ano, :].groupby('CO_MN_INF')['CO_MN_INF'].count().rename(ano)
        ], axis=1)
       
    for idx in hiv.index:
        try:
            hiv.loc[idx] = hiv.loc[idx] / casos.loc[idx] * 100
        except:
            print(idx)
        
    return hiv.fillna(0)

def proporcao_casos_cura_clinica_20(df):
    """
    | Importância do indicador:
    | -Analisar variações populacionais, geográficas e temporais e subsidiar
    | processos de planejamento, gestão e avaliação de políticas e ações de saúde direcionadas ao controle
    | da LV.
    | 
    | Método de cálculo:
    | - Número total de casos novos de LV que evoluiram para cura clínica agrupados por local de residência (município) 
    | no ano de notificação dividido por número total de casos novos de LV por local de residência (município) 
    | no ano de notificação x 100
    """
    data = df.loc[
        (df.ENTRADA == 1) & 
        (df.EVOLUCAO == 1) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    casos = total_casos_novos_municipio_residencia_ano_14(df)

    cura = pd.DataFrame(index=mun)
    for ano in anos:
        cura = pd.concat([
            cura,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
    
    for idx in cura.index:
        try:
            cura.loc[idx] = cura.loc[idx] / casos.loc[idx] * 100
        except:
            print(idx)
        
    return cura.fillna(0)

def total_obitos_residencia_ano_21(df):
    '''
    | Importância do indicador: Permite avaliar as ações de vigilância e assistência.
    | Indicado para unidades territoriais (estados e municípios) com menos de 20 casos de LV por ano.
    |
    | Número de óbitos por LV dentre o total de casos novos e recidivas, por local de residência (UF,
    | município, RA ou localidade) no ano de notificação
    '''
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.EVOLUCAO == 3) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    obitos = pd.DataFrame(index=mun)
    for ano in anos:
        obitos = pd.concat([
            obitos,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
    
    return obitos.fillna(0).astype(int)
    

def taxa_letalidade_municipio_residencia_ano_22(df):
    """
    | Importância do indicador:
    | - Está relacionado com o diagnóstico precoce e o tratamento e acompanhamento adequado dos
    | pacientes com LV.
    | - Permite avaliar de forma indireta as ações de vigilância e assistência.
    | 
    | Método de cálculo:
    | - Número total de óbitos por LV agrupados por local de residência (município) 
    | no ano de notificação dividido por número total de casos (novos e recidivas) de LV 
    | por local de residência (município) no ano de notificação x 100
    """
    data =  df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort() 
    obitos = total_obitos_residencia_ano_21(df)    
    casos_novos_rescidivas = pd.DataFrame(index=mun)
    for ano in anos:
        casos_novos_rescidivas = pd.concat([
            casos_novos_rescidivas,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
    
    for idx in obitos.index:
        try:
            obitos.loc[idx] = obitos.loc[idx] / casos_novos_rescidivas.loc[idx] * 100
        except:
            print(idx)
        
    return obitos.fillna(0)

def taxa_letalidade_hiv_municipio_residencia_ano_23(df):
    """
    | Importância do indicador:
    | - Está relacionado com o diagnóstico precoce e o tratamento e acompanhamento adequado dos
    | pacientes com LV.
    | - Permite avaliar de forma indireta as ações de vigilância e assistência.
    | 
    | Método de cálculo:
    | - Número total de óbitos por LV em coinfectados por HIV agrupados por local de residência (município) 
    | no ano de notificação dividido por número total de casos (novos e recidivas) de LV 
    | por local de residência (município) no ano de notificação x 100
    """
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.EVOLUCAO == 3) &
        (df.HIV == 1) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort()
       
    obitos_hiv = pd.DataFrame(index=mun)
    for ano in anos:
        obitos_hiv = pd.concat([
            obitos_hiv,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)        
                     
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.HIV == 1) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()   
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    casos_hiv = pd.DataFrame(index=mun)
    for ano in anos:
        casos_hiv = pd.concat([
            casos_hiv,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)    
     
    for idx in obitos_hiv.index:
        try:
            obitos_hiv.loc[idx] = obitos_hiv.loc[idx] / casos_hiv.loc[idx] * 100
        except:
            print(idx)
        
    return obitos_hiv.fillna(0)
    
def taxa_letalidade_menor_5_anos_municipio_residencia_ano_24(df):
    """
    | Importância do indicador:
    | - Permite analisar o risco de óbitos por LV em menores de 5 anos em
    | comparação com a população geral para direcionar e priorizar as ações de vigilância, prevenção e
    | controle.
    | 
    | Indicado para unidades territoriais (estados e municípios) com mais de 20 casos de LV em menores
    | de 5 anos por ano.
    | 
    | Método de cálculo:
    | - Número total de óbitos por LV em menores de 5 anos agrupados por local de residência (município) 
    | no ano de notificação dividido por número total de casos (novos e recidivas) de LV 
    | em menores de 5 anos por local de residência (município) no ano de notificação x 100
    """
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.EVOLUCAO == 3) &
        (df.IDADE < 5) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort()       
    obitos_menor_5_anos = pd.DataFrame(index=mun)

    for ano in anos:
        obitos_menor_5_anos = pd.concat([
            obitos_menor_5_anos,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)   
        
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.IDADE < 5) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()   
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    casos_menor_5_anos = pd.DataFrame(index=mun)
    for ano in anos:
        casos_menor_5_anos = pd.concat([
            casos_menor_5_anos,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
            
    for idx in obitos_menor_5_anos.index:
        try:
            obitos_menor_5_anos.loc[idx] = obitos_menor_5_anos.loc[idx] / casos_menor_5_anos.loc[idx] * 100
        except:
            print(idx)
        
    return obitos_menor_5_anos.fillna(0)

def taxa_letalidade_50_anos_mais_municipio_residencia_ano_25(df):
    """
    | Importância do indicador:
    | - Permite analisar o risco de óbitos por LV em maiores de 50 anos em
    | comparação com a população geral par direcionar e priorizar as ações de vigilância, prevenção e
    | controle.
    | 
    | Indicado para unidades territoriais (estados e municípios) com mais de 20 casos de LV em maiores
    | de 50 anos por ano.
    | 
    | Método de cálculo:
    | - Número total de óbitos por LV em >= 50 anos agrupados por local de residência (município) 
    | no ano de notificação dividido por número total de casos (novos e recidivas) de LV 
    | em >= 50 anos por local de residência (município) no ano de notificação x 100
    """
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.EVOLUCAO == 3) &
        (df.IDADE >= 50) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy() 
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort()       
    obitos_50_anos_mais = pd.DataFrame(index=mun)

    for ano in anos:
        obitos_50_anos_mais = pd.concat([
            obitos_50_anos_mais,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
      
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.IDADE >= 50) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    casos_50_anos_mais = pd.DataFrame(index=mun)
    for ano in anos:
        casos_50_anos_mais = pd.concat([
            casos_50_anos_mais,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)   
    
    for idx in obitos_50_anos_mais.index:
        try:
            obitos_50_anos_mais.loc[idx] = obitos_50_anos_mais.loc[idx] / casos_50_anos_mais.loc[idx] * 100
        except:
            print(idx)
        
    return obitos_50_anos_mais.fillna(0)

def proporcao_casos_evolucao_ignorada_branco_ano_26(df):
    """
    | Importância do indicador:
    | - Permite analisar o acompanhamento e encerramento dos casos de LV.
    | - Este indicador impacta nos resultados dos demais indicadores de evolução.
    | 
    | Método de cálculo:
    | - Número total de casos de LV (novos e rescidivas) com evolução ignorada ou em branco agrupados por local de residência 
    | (município) no ano de notificação dividido por número total de casos (novos e recidivas) de LV 
    | por local de residência (município) no ano de notificação x 100
    """
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (df.EVOLUCAO.isnull()) &
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    mun = pd.read_csv('../data/processed/processed_municipios.csv').ibge_code.values
    anos = data['ANO'].unique()
    mun.sort()
    anos.sort()       
    evolucao_null = pd.DataFrame(index=mun)
    
    for ano in anos:
        evolucao_null = pd.concat([
            evolucao_null,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)            
    
    data = df.loc[
        ((df.ENTRADA == 1) | (df.ENTRADA == 2)) & 
        (~df.CO_MN_RESI.isnull()) & 
        (~df.CO_MN_RESI.astype('string').str.contains('0000')), 
        :
    ].copy()    
    data.CO_MN_RESI = data.CO_MN_RESI.astype(int)        
    casos_novos_rescidivas = pd.DataFrame(index=mun)
    for ano in anos:
        casos_novos_rescidivas = pd.concat([
            casos_novos_rescidivas,
            data.loc[data.ANO == ano, :].groupby('CO_MN_RESI')['CO_MN_RESI'].count().rename(ano)
        ], axis=1)
    
    for idx in evolucao_null.index:
        try:
            evolucao_null.loc[idx] = evolucao_null.loc[idx] / casos_novos_rescidivas.loc[idx] * 100
        except:
            print(idx)
        
    return evolucao_null.fillna(0)