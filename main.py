from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import pandas as pd
import sys

# --- CONFIGURAÇÃO ---
# ATENÇÃO: Substitua pela sua Chave de API REAL. 
# Por segurança, o ideal é carregar chaves de ambientes ou variáveis de ambiente.
YOUTUBE_API_KEY = "" 
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

if not YOUTUBE_API_KEY:
    print("ERRO: Por favor, insira sua YOUTUBE_API_KEY real no campo de CONFIGURAÇÃO.")
    # Usar sys.exit(1) para parar a execução sem a chave
    # No entanto, em um ambiente de execução isolado, apenas imprimir o erro pode ser preferível.
    # sys.exit(1)

# Inicializa o cliente da API
try:
    youtube = build(
        YOUTUBE_API_SERVICE_NAME, 
        YOUTUBE_API_VERSION, 
        developerKey=YOUTUBE_API_KEY,
        # Define um tempo limite de 5 segundos para a conexão inicial
        cache_discovery=False,
        request_args={'timeout': 5}
    )
except Exception as e:
    print(f"ERRO: Não foi possível inicializar o cliente da API do YouTube. Detalhes: {e}")
    sys.exit(1)


# --- LISTA DE EVENTOS WR (Para Iteração) ---
# Você pode expandir ou reduzir esta lista.
# O formato de data é YYYY-MM-DD.
EVENTOS_WR = [
    {"runner": "sutemou", "data": "2022-06-17", "tempo": "1h 08m 48s"},
    {"runner": "KEMIST_C10H15N", "data": "2022-08-31", "tempo": "59m 41s"}, # Sub-1h
    {"runner": "Distortion2", "data": "2023-03-01", "tempo": "58m 35s"},
    # Adicione mais eventos da sua lista conforme necessário...
]


def buscar_videos_por_periodo(query, data_limite, antes_depois="depois", max_videos=2):
    """
    Busca os vídeos mais relevantes ANTES ou DEPOIS de uma data limite.
    Retorna uma lista de IDs de vídeo.
    """
    try:
        # Formata a data para o padrão ISO 8601 (requerido pela API)
        data_dt = datetime.strptime(data_limite, "%Y-%m-%d")
        
        # O filtro de data é ligeiramente ajustado para garantir uma separação limpa:
        # DEPOIS: A partir do dia do WR (inclusive)
        # ANTES: Até o dia anterior ao WR (exclusive)
        date_filter = {}
        if antes_depois == "antes":
            # Busca vídeos publicados até um segundo antes da data do WR começar
            data_limite_antes = data_dt - timedelta(seconds=1)
            data_iso = data_limite_antes.isoformat() + 'Z'
            date_filter['publishedBefore'] = data_iso
        else: # depois
            data_iso = data_dt.isoformat() + 'Z'
            date_filter['publishedAfter'] = data_iso
            
        print(f"\n-> Buscando {max_videos} vídeos {antes_depois} de {data_limite}...")

        # Chamada à API: search:list
        search_response = youtube.search().list(
            q=query,
            part="id,snippet",
            maxResults=max_videos,
            # Ordena por relevância (relevance) ou data (date). 
            # 'date' é bom para vídeos próximos ao evento.
            order="date", 
            type="video",
            **date_filter
        ).execute()
        
        video_ids = []
        for search_result in search_response.get("items", []):
            video_ids.append(search_result["id"]["videoId"])
            
        return video_ids
        
    except HttpError as e:
        print(f"ERRO DE API durante a busca: {e}")
        return []
    except Exception as e:
        print(f"Um erro inesperado ocorreu durante a busca: {e}")
        return []


def obter_estatisticas_videos(video_ids):
    """
    Coleta o viewCount e a data de publicação de uma lista de IDs de vídeo.
    Retorna uma lista de dicionários com os dados.
    """
    if not video_ids:
        return []
    
    try:
        # Chamada à API: videos:list
        videos_response = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(video_ids) # Junta todos os IDs com vírgula
        ).execute()

        dados_coletados = []
        for item in videos_response.get("items", []):
            # Tenta obter a contagem de views, tratando o caso em que ela não existe
            view_count = int(item["statistics"].get("viewCount", 0))
            
            dados_coletados.append({
                "titulo": item["snippet"]["title"],
                "data_publicacao": item["snippet"]["publishedAt"],
                "visualizacoes": view_count,
                "video_id": item["id"]
            })
            
        return dados_coletados

    except HttpError as e:
        print(f"ERRO DE API ao obter estatísticas dos vídeos: {e}")
        return []
    except Exception as e:
        print(f"Um erro inesperado ocorreu ao obter estatísticas: {e}")
        return []


# --- EXECUÇÃO PRINCIPAL ---
todos_os_dados = []

# Verifica se a chave de API foi inserida antes de prosseguir
if not YOUTUBE_API_KEY:
    print("\nExecução abortada. Por favor, forneça uma chave de API válida.")
else:
    for evento in EVENTOS_WR:
        data_wr = evento["data"]
        runner = evento["runner"]
        tempo = evento["tempo"]
        
        print(f"\n--- Processando Evento WR: {runner} em {data_wr} ({tempo}) ---")

        # 1. BUSCA - Vídeos ANTES (Busca mais genérica para controle)
        query_antes = f"Elden Ring speedrun Any%"
        ids_antes = buscar_videos_por_periodo(query_antes, data_wr, antes_depois="antes", max_videos=2)

        # 2. BUSCA - Vídeos DEPOIS (Busca específica sobre o novo recorde)
        query_depois = f"Elden Ring WR {runner} {tempo}" 
        ids_depois = buscar_videos_por_periodo(query_depois, data_wr, antes_depois="depois", max_videos=2)
        
        # 3. COLETA - Estatísticas dos vídeos ANTES
        dados_antes = obter_estatisticas_videos(ids_antes)
        for dado in dados_antes:
            dado['evento_wr_data'] = data_wr
            dado['periodo'] = 'ANTES'
            todos_os_dados.append(dado)

        # 4. COLETA - Estatísticas dos vídeos DEPOIS
        dados_depois = obter_estatisticas_videos(ids_depois)
        for dado in dados_depois:
            dado['evento_wr_data'] = data_wr
            dado['periodo'] = 'DEPOIS'
            todos_os_dados.append(dado)


    # --- EXIBIÇÃO E EXPORTAÇÃO (Para Análise) ---

    if todos_os_dados:
        # Converte a lista de dicionários para um DataFrame do Pandas
        df = pd.DataFrame(todos_os_dados)

        # Garante que a coluna de visualizações é numérica (embora já seja int, é bom garantir)
        df['visualizacoes'] = pd.to_numeric(df['visualizacoes'], errors='coerce')

        # Analisa a média de visualizações por período para cada evento WR
        analise_final = df.groupby(['evento_wr_data', 'periodo'])['visualizacoes'].mean().reset_index()
        analise_final.rename(columns={'visualizacoes': 'media_visualizacoes'}, inplace=True)


        print("\n" + "="*50)
        print("--- RESULTADOS FINAIS POR EVENTO WR (Média de Views) ---")
        print("="*50)
        # Imprime a análise de forma mais limpa usando o Pandas to_string()
        print(analise_final.to_string(index=False))

        # >>> EXPORTAÇÃO FINAL PARA ARQUIVO CSV <<<
        
        try:
            # 1. Salva TODOS os vídeos coletados (antes e depois) no arquivo CSV
            df.to_csv("elden_ring_youtube_dados_completos.csv", index=False, encoding='utf-8')
            print("\nArquivo 'elden_ring_youtube_dados_completos.csv' salvo com sucesso!")

            # 2. Salva apenas a tabela de médias de análise
            analise_final.to_csv("elden_ring_youtube_analise_medias.csv", index=False, encoding='utf-8')
            print("Arquivo 'elden_ring_youtube_analise_medias.csv' salvo com sucesso!")
        
        except Exception as e:
            print(f"ERRO: Não foi possível exportar os arquivos CSV. Detalhes: {e}")

    else:
        print("\nNenhum dado foi coletado. Verifique sua chave de API e os limites de cota.")
