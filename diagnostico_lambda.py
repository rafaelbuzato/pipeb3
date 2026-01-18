# diagnostico_lambda.py
import boto3

lambda_client = boto3.client('lambda', region_name='us-east-1')

print("="*60)
print("DIAGNÓSTICO - LAMBDAS DISPONÍVEIS")
print("="*60)

try:
    # Listar todas as Lambdas
    response = lambda_client.list_functions()
    
    if response['Functions']:
        print(f"\n✓ Encontradas {len(response['Functions'])} Lambdas:\n")
        for func in response['Functions']:
            print(f"  - {func['FunctionName']}")
            print(f"    ARN: {func['FunctionArn']}")
            print(f"    Runtime: {func['Runtime']}")
            print(f"    Última atualização: {func['LastModified']}")
            print()
    else:
        print("\n⚠ Nenhuma Lambda encontrada na região us-east-1")
        print("Você precisa criar a Lambda de ingestão!")
        
except Exception as e:
    print(f"\n✗ Erro ao listar Lambdas: {e}")

print("="*60)