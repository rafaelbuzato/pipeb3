import yfinance as yf
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class B3Extractor:
    """Extrator de dados da B3 - Ações e Índices"""
    
    def __init__(self):
        # Ativos que sempre têm dados disponíveis
        self.ativos = {
            'IBOV': '^BVSP',      # Índice Bovespa
            'PETR4': 'PETR4.SA',  # Petrobras
            'VALE3': 'VALE3.SA',  # Vale
            'ITUB4': 'ITUB4.SA',  # Itaú
            'BBDC4': 'BBDC4.SA',  # Bradesco
        }
    
    def extract_data(self, ativo='IBOV', period='1mo', interval='1d'):
        """Extrai dados de um ativo"""
        
        if ativo in self.ativos:
            ticker_symbol = self.ativos[ativo]
        else:
            ticker_symbol = ativo  # Usar símbolo direto
        
        logger.info(f'Extraindo {ticker_symbol}...')
        
        try:
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period=period, interval=interval)
            
            if df.empty:
                logger.warning(f'Sem dados para {ticker_symbol}')
                return pd.DataFrame()
            
            # Processar
            df = df.reset_index()
            df = df.rename(columns={
                'Date': 'data',
                'Open': 'abertura',
                'High': 'maxima',
                'Low': 'minima',
                'Close': 'fechamento',
                'Volume': 'volume'
            })
            
            df['ticker'] = ativo
            df['simbolo'] = ticker_symbol
            df = df.drop(columns=['Dividends', 'Stock Splits'], errors='ignore')
            
            logger.info(f'OK! {len(df)} registros extraídos')
            return df
            
        except Exception as e:
            logger.error(f'Erro: {e}')
            return pd.DataFrame()
    
    def extract_multiple(self, ativos=None, period='1mo', interval='1d'):
        """Extrai dados de múltiplos ativos"""
        
        if ativos is None:
            ativos = list(self.ativos.keys())
        
        all_data = []
        
        for ativo in ativos:
            df = self.extract_data(ativo, period, interval)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def save_to_parquet(self, df, filename):
        """Salva em Parquet"""
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(filename, index=False)
        logger.info(f'Salvo: {filename}')
    
    def save_to_csv(self, df, filename):
        """Salva em CSV"""
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f'Salvo: {filename}')

if __name__ == '__main__':
    print('='*70)
    print('EXTRATOR B3 - AÇÕES E ÍNDICES')
    print('='*70)
    
    extractor = B3Extractor()
    
    # Método 1: Extrair Ibovespa apenas
    print('\n1. Extraindo Ibovespa (último mês)...')
    df_ibov = extractor.extract_data('IBOV', period='1mo', interval='1d')
    
    if not df_ibov.empty:
        print(f'\n✓ Ibovespa: {len(df_ibov)} registros')
        print(df_ibov[['data', 'ticker', 'fechamento', 'volume']].tail())
        
        extractor.save_to_parquet(df_ibov, 'data/ibov_1m.parquet')
        extractor.save_to_csv(df_ibov, 'data/ibov_1m.csv')
    
    # Método 2: Extrair múltiplas ações
    print('\n2. Extraindo múltiplas ações (últimos 5 dias)...')
    df_multi = extractor.extract_multiple(
        ativos=['IBOV', 'PETR4', 'VALE3'],
        period='5d',
        interval='1d'
    )
    
    if not df_multi.empty:
        print(f'\n✓ Total: {len(df_multi)} registros')
        print('\nResumo por ativo:')
        print(df_multi.groupby('ticker').size())
        
        extractor.save_to_parquet(df_multi, 'data/b3_multiplos.parquet')
        extractor.save_to_csv(df_multi, 'data/b3_multiplos.csv')
    
    print('\n' + '='*70)
    print('EXTRAÇÃO CONCLUÍDA!')
    print('Arquivos salvos em: data/')
    print('='*70)
