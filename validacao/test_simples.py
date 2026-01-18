import yfinance as yf
import pandas as pd

print("=" * 50)
print("TESTE SIMPLES DE EXTRAÇÃO")
print("=" * 50)

# Teste 1: Ibovespa (sempre funciona)
print("\n1. Testando Ibovespa...")
try:
    ibov = yf.Ticker("^BVSP")
    df_ibov = ibov.history(period="5d")
    print(f"   OK! {len(df_ibov)} registros")
    print(df_ibov[['Close', 'Volume']].tail(3))
except Exception as e:
    print(f"   ERRO: {e}")

# Teste 2: Ação brasileira
print("\n2. Testando PETR4...")
try:
    petr = yf.Ticker("PETR4.SA")
    df_petr = petr.history(period="5d")
    print(f"   OK! {len(df_petr)} registros")
    print(df_petr[['Close', 'Volume']].tail(3))
except Exception as e:
    print(f"   ERRO: {e}")

# Teste 3: WIN Futuro
print("\n3. Testando WINF26...")
try:
    win = yf.Ticker("WINF26.SA")
    df_win = win.history(period="5d")
    if len(df_win) > 0:
        print(f"   OK! {len(df_win)} registros")
        print(df_win[['Close', 'Volume']].tail(3))
    else:
        print("   Contrato sem dados (pode estar vencido)")
except Exception as e:
    print(f"   ERRO: {e}")

# Teste 4: Tentar outros contratos WIN
print("\n4. Testando outros contratos WIN...")
contratos = ["WING26.SA", "WINH26.SA", "WINJ26.SA"]
for contrato in contratos:
    try:
        ticker = yf.Ticker(contrato)
        df = ticker.history(period="5d")
        if len(df) > 0:
            print(f"   {contrato}: OK! {len(df)} registros")
            break
    except:
        print(f"   {contrato}: sem dados")

print("\n" + "=" * 50)
print("TESTE CONCLUÍDO")
print("=" * 50)