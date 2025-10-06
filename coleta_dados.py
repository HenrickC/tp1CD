# coleta_dados.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
from googleapiclient.discovery import build
import re

# Importa a chave da API do arquivo de configuração
try:
    from config import YOUTUBE_API_KEY
except ImportError:
    print("Erro: Arquivo 'config.py' não encontrado ou a variável YOUTUBE_API_KEY não foi definida.")
    print("Crie um arquivo chamado config.py e adicione a linha: YOUTUBE_API_KEY = 'SUA_CHAVE_AQUI'")
    exit() # Encerra o script se a chave não for encontrada

def extrair_dados_speedrun(url):
    """Função para extrair dados da tabela do speedrun.com."""
    print("Iniciando coleta de dados do Speedrun.com...")
    
    # Bloco de headers aprimorado para simular um navegador real e evitar bloqueios.
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    
    try:
        # Requisição com headers completos e um timeout de 15 segundos.
        response = requests.get(url, headers=headers, timeout=15)
        # Lança um erro para status codes ruins (como 403, 404, 500 etc)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"Falha ao acessar a URL. Erro: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='table-condensed')
    if not table:
        print("Erro: Tabela de speedruns não encontrada no HTML. O site pode ter mudado sua estrutura.")
        return None
        
    runs_data = []

    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) > 5:
            rank = cols[0].get_text(strip=True)
            player = cols[1].get_text(strip=True)
            time = cols[2].get_text(strip=True)
            platform = cols[3].get_text(strip=True)
            date = cols[4].get_text(strip=True)
            video_url = cols[5].find('a')['href'] if cols[5].find('a') else 'N/A'
            runs_data.append([rank, player, time, platform, date, video_url])

    print(f"{len(runs_data)} runs encontradas e extraídas com sucesso.")
    return pd.DataFrame(runs_data, columns=['Rank', 'Player', 'Time', 'Platform', 'Date', 'Video_URL'])

def obter_estatisticas_youtube(video_url, api_key):
    """Busca estatísticas de um vídeo do YouTube usando a API."""
    if 'youtube.com' not in video_url and 'youtu.be' not in video_url:
        return None, None, None

    video_id_match = re.search(r'(?<=v=)[\w-]+|(?<=be/)[\w-]+', video_url)
    if not video_id_match:
        return None, None, None
    
    video_id = video_id_match.group(0)
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.videos().list(part="statistics", id=video_id)
        response = request.execute()

        if 'items' in response and response['items']:
            stats = response['items'][0]['statistics']
            return (
                int(stats.get('viewCount', 0)),
                int(stats.get('likeCount', 0)),
                int(stats.get('commentCount', 0))
            )
    except Exception as e:
        print(f"  -> Erro ao processar vídeo ID {video_id}: {e}")
    
    return None, None, None

def main():
    """Função principal que orquestra todo o processo."""
    url_speedrun = "https://www.speedrun.com/eldenring?h=Any-glitchless&x=02qr00pk-7891zr5n.qj740p3q"
    
    df = extrair_dados_speedrun(url_speedrun)
    
    if df is None or df.empty:
        print("Nenhum dado foi extraído do Speedrun.com. Encerrando o script.")
        return

    print("\nBuscando dados no YouTube para cada run (isso pode levar um tempo)...")
    
    youtube_stats = df['Video_URL'].apply(lambda url: pd.Series(obter_estatisticas_youtube(url, YOUTUBE_API_KEY)))
    youtube_stats.columns = ['Views', 'Likes', 'Comments']
    
    df_final = pd.concat([df, youtube_stats], axis=1)

    print("\nDados enriquecidos com sucesso.")
    
    output_filename = 'analise_speedruns_elden_ring.csv'
    df_final.to_csv(output_filename, index=False, encoding='utf-8')
    
    print(f"\nProcesso finalizado! Os dados foram salvos em '{output_filename}'")
    print("\nVisualização das 5 primeiras linhas do resultado final:")
    print(df_final.head())

# Executa a função principal quando o script é chamado
if __name__ == "__main__":
    main()
