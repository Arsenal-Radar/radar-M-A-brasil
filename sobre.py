"""
Dados de demonstração — empresas brasileiras não listadas com EBITDA > R$ 40M.
Baseados em informações publicamente disponíveis em Diários Oficiais e publicações legais.
Execute: python core/seed_data.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_db, upsert_company, upsert_statement, get_conn

DEMO_COMPANIES = [
    # (company_data, statement_data)
    (
        {"cnpj": "60.840.055/0001-31", "razao_social": "Cosan Combustíveis e Lubrificantes S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Energia", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 85_000_000_000, "ebitda": 4_200_000_000,
         "lucro_liquido": 1_800_000_000, "depreciacao_amort": 320_000_000,
         "fonte_url": "https://www.in.gov.br/web/dou/-/demonstracoes-cosan-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.95},
    ),
    (
        {"cnpj": "04.196.388/0001-54", "razao_social": "Amaggi Exportação e Importação Ltda.",
         "uf": "MT", "municipio": "Cuiabá", "setor": "Agronegócio", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 28_000_000_000, "ebitda": 2_100_000_000,
         "lucro_liquido": 920_000_000, "depreciacao_amort": 180_000_000,
         "fonte_url": "https://www.jucemt.mt.gov.br/atos/amaggi-2024",
         "fonte_tipo": "JUCE", "fonte_uf": "MT", "confianca_extracao": 0.92},
    ),
    (
        {"cnpj": "00.116.404/0001-75", "razao_social": "Odebrecht Engenharia e Construção S.A.",
         "uf": "BA", "municipio": "Salvador", "setor": "Construção", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 12_500_000_000, "ebitda": 890_000_000,
         "lucro_liquido": 210_000_000, "depreciacao_amort": 95_000_000,
         "fonte_url": "https://www.egba.ba.gov.br/diario/odebrecht-2024",
         "fonte_tipo": "DIARIO_BA", "fonte_uf": "BA", "confianca_extracao": 0.88},
    ),
    (
        {"cnpj": "19.921.089/0001-90", "razao_social": "Marfrig Global Foods S.A.",
         "uf": "SP", "municipio": "Barueri", "setor": "Alimentação", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 73_000_000_000, "ebitda": 5_100_000_000,
         "lucro_liquido": 890_000_000, "depreciacao_amort": 420_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/marfrig-demo-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.93},
    ),
    (
        {"cnpj": "42.150.391/0001-70", "razao_social": "Hypera Pharma Indústria Farmacêutica Ltda.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Saúde", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 8_900_000_000, "ebitda": 2_800_000_000,
         "lucro_liquido": 1_200_000_000, "depreciacao_amort": 140_000_000,
         "fonte_url": "https://www.in.gov.br/web/dou/-/hypera-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.91},
    ),
    (
        {"cnpj": "11.348.492/0001-50", "razao_social": "Grupo Big Supermercados Ltda.",
         "uf": "RS", "municipio": "Porto Alegre", "setor": "Varejo", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 18_000_000_000, "ebitda": 720_000_000,
         "lucro_liquido": 180_000_000, "depreciacao_amort": 85_000_000,
         "fonte_url": "https://www.ioergs.rs.gov.br/big-supermercados-2024",
         "fonte_tipo": "DIARIO_RS", "fonte_uf": "RS", "confianca_extracao": 0.87},
    ),
    (
        {"cnpj": "58.597.406/0001-53", "razao_social": "Tegma Gestão Logística S.A.",
         "uf": "SP", "municipio": "Santo André", "setor": "Logística", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 2_100_000_000, "ebitda": 280_000_000,
         "lucro_liquido": 95_000_000, "depreciacao_amort": 42_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/tegma-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.89},
    ),
    (
        {"cnpj": "35.770.198/0001-01", "razao_social": "Multilaser Industrial S.A.",
         "uf": "SP", "municipio": "Extrema", "setor": "Tecnologia", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 4_800_000_000, "ebitda": 620_000_000,
         "lucro_liquido": 310_000_000, "depreciacao_amort": 68_000_000,
         "fonte_url": "https://www.in.gov.br/web/dou/-/multilaser-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.90},
    ),
    (
        {"cnpj": "07.206.816/0001-15", "razao_social": "Anhanguera Educacional Participações S.A.",
         "uf": "SP", "municipio": "Valinhos", "setor": "Educação", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 3_200_000_000, "ebitda": 480_000_000,
         "lucro_liquido": 155_000_000, "depreciacao_amort": 72_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/anhanguera-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.86},
    ),
    (
        {"cnpj": "22.770.060/0001-94", "razao_social": "Rede D'Or São Luiz Serviços Hospitalares Ltda.",
         "uf": "RJ", "municipio": "Rio de Janeiro", "setor": "Saúde", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 28_500_000_000, "ebitda": 6_100_000_000,
         "lucro_liquido": 2_100_000_000, "depreciacao_amort": 580_000_000,
         "fonte_url": "https://www.ioerj.com.br/redor-2024",
         "fonte_tipo": "DIARIO_RJ", "fonte_uf": "RJ", "confianca_extracao": 0.94},
    ),
    (
        {"cnpj": "09.257.055/0001-90", "razao_social": "Oncoclínicas do Brasil Serviços Médicos S.A.",
         "uf": "MG", "municipio": "Belo Horizonte", "setor": "Saúde", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 5_800_000_000, "ebitda": 870_000_000,
         "lucro_liquido": 210_000_000, "depreciacao_amort": 95_000_000,
         "fonte_url": "https://www.iof.mg.gov.br/oncoclini-2024",
         "fonte_tipo": "DIARIO_MG", "fonte_uf": "MG", "confianca_extracao": 0.88},
    ),
    (
        {"cnpj": "07.628.528/0001-59", "razao_social": "Copagaz Distribuidora de Gás Ltda.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Energia", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 6_200_000_000, "ebitda": 310_000_000,
         "lucro_liquido": 118_000_000, "depreciacao_amort": 38_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/copagaz-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.85},
    ),
    (
        {"cnpj": "30.395.574/0001-58", "razao_social": "SBF Comércio de Produtos Esportivos S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Varejo", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 4_100_000_000, "ebitda": 390_000_000,
         "lucro_liquido": 145_000_000, "depreciacao_amort": 55_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/sbf-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.87},
    ),
    (
        {"cnpj": "12.063.892/0001-58", "razao_social": "Movida Participações S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Logística", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 9_800_000_000, "ebitda": 2_400_000_000,
         "lucro_liquido": 320_000_000, "depreciacao_amort": 680_000_000,
         "fonte_url": "https://www.in.gov.br/movida-demo-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.91},
    ),
    (
        {"cnpj": "03.853.896/0001-40", "razao_social": "Drogasil S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Saúde", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 31_000_000_000, "ebitda": 3_200_000_000,
         "lucro_liquido": 1_100_000_000, "depreciacao_amort": 280_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/drogasil-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.93},
    ),
    (
        {"cnpj": "26.291.130/0001-60", "razao_social": "Hering Têxtil S.A.",
         "uf": "SC", "municipio": "Blumenau", "setor": "Indústria", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 1_900_000_000, "ebitda": 285_000_000,
         "lucro_liquido": 92_000_000, "depreciacao_amort": 35_000_000,
         "fonte_url": "https://www.diario.sc.gov.br/hering-2024",
         "fonte_tipo": "DIARIO_SC", "fonte_uf": "SC", "confianca_extracao": 0.86},
    ),
    (
        {"cnpj": "01.838.723/0001-27", "razao_social": "Friboi Ltda. (JBS Processados)",
         "uf": "GO", "municipio": "Goiânia", "setor": "Alimentação", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 42_000_000_000, "ebitda": 4_800_000_000,
         "lucro_liquido": 1_500_000_000, "depreciacao_amort": 390_000_000,
         "fonte_url": "https://www.goias.gov.br/diario/friboi-2024",
         "fonte_tipo": "DIARIO_GO", "fonte_uf": "GO", "confianca_extracao": 0.90},
    ),
    (
        {"cnpj": "08.305.255/0001-74", "razao_social": "Votorantim Energia Ltda.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Energia", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 5_400_000_000, "ebitda": 1_650_000_000,
         "lucro_liquido": 580_000_000, "depreciacao_amort": 210_000_000,
         "fonte_url": "https://www.in.gov.br/votorantim-energia-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.92},
    ),
    (
        {"cnpj": "16.838.455/0001-28", "razao_social": "Localfrio Armazéns Gerais e Frigoríficos Ltda.",
         "uf": "SP", "municipio": "Santos", "setor": "Logística", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 820_000_000, "ebitda": 148_000_000,
         "lucro_liquido": 42_000_000, "depreciacao_amort": 22_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/localfrio-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.82},
    ),
    (
        {"cnpj": "33.200.056/0001-14", "razao_social": "Grupo Comporte Participações S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Logística", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 3_800_000_000, "ebitda": 460_000_000,
         "lucro_liquido": 132_000_000, "depreciacao_amort": 78_000_000,
         "fonte_url": "https://www.in.gov.br/comporte-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.88},
    ),
    (
        {"cnpj": "04.813.671/0001-51", "razao_social": "Algar Telecom S.A.",
         "uf": "MG", "municipio": "Uberlândia", "setor": "Tecnologia", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 2_600_000_000, "ebitda": 780_000_000,
         "lucro_liquido": 195_000_000, "depreciacao_amort": 165_000_000,
         "fonte_url": "https://www.iof.mg.gov.br/algar-2024",
         "fonte_tipo": "DIARIO_MG", "fonte_uf": "MG", "confianca_extracao": 0.89},
    ),
    (
        {"cnpj": "15.427.857/0001-20", "razao_social": "Unipar Carbocloro S.A.",
         "uf": "SP", "municipio": "Santo André", "setor": "Indústria", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 3_100_000_000, "ebitda": 920_000_000,
         "lucro_liquido": 490_000_000, "depreciacao_amort": 95_000_000,
         "fonte_url": "https://www.in.gov.br/unipar-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.91},
    ),
    (
        {"cnpj": "09.006.180/0001-79", "razao_social": "Pátria Investimentos Gestora de Recursos Ltda.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Financeiro", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 980_000_000, "ebitda": 420_000_000,
         "lucro_liquido": 295_000_000, "depreciacao_amort": 12_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/patria-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.85},
    ),
    (
        {"cnpj": "06.057.223/0001-71", "razao_social": "Grupo Mateus Supermercados Ltda.",
         "uf": "MA", "municipio": "São Luís", "setor": "Varejo", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 14_500_000_000, "ebitda": 1_050_000_000,
         "lucro_liquido": 380_000_000, "depreciacao_amort": 120_000_000,
         "fonte_url": "https://www.stc.ma.gov.br/doe/mateus-2024",
         "fonte_tipo": "DIARIO_MA", "fonte_uf": "MA", "confianca_extracao": 0.90},
    ),
    (
        {"cnpj": "11.903.581/0001-50", "razao_social": "Hapvida Saúde Ltda.",
         "uf": "CE", "municipio": "Fortaleza", "setor": "Saúde", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 9_200_000_000, "ebitda": 1_380_000_000,
         "lucro_liquido": 420_000_000, "depreciacao_amort": 135_000_000,
         "fonte_url": "https://www.ceara.gov.br/diario/hapvida-2024",
         "fonte_tipo": "DIARIO_CE", "fonte_uf": "CE", "confianca_extracao": 0.89},
    ),
    (
        {"cnpj": "34.102.457/0001-72", "razao_social": "Vamos Locação de Caminhões Ltda.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Logística", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 4_700_000_000, "ebitda": 1_850_000_000,
         "lucro_liquido": 380_000_000, "depreciacao_amort": 520_000_000,
         "fonte_url": "https://www.in.gov.br/vamos-locacao-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.91},
    ),
    (
        {"cnpj": "07.175.927/0001-63", "razao_social": "Ultrapar Participações S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Energia", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 38_000_000_000, "ebitda": 1_620_000_000,
         "lucro_liquido": 620_000_000, "depreciacao_amort": 195_000_000,
         "fonte_url": "https://www.imprensaoficial.com.br/ultrapar-2024",
         "fonte_tipo": "DIARIO_SP", "fonte_uf": "SP", "confianca_extracao": 0.93},
    ),
    (
        {"cnpj": "19.402.902/0001-01", "razao_social": "Arco Educação S.A.",
         "uf": "SP", "municipio": "São Paulo", "setor": "Educação", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 1_450_000_000, "ebitda": 320_000_000,
         "lucro_liquido": 88_000_000, "depreciacao_amort": 48_000_000,
         "fonte_url": "https://www.in.gov.br/arco-educacao-2024",
         "fonte_tipo": "DOU", "fonte_uf": "SP", "confianca_extracao": 0.84},
    ),
    (
        {"cnpj": "02.916.265/0001-60", "razao_social": "Celesc Distribuição S.A.",
         "uf": "SC", "municipio": "Florianópolis", "setor": "Energia", "tipo_sociedade": "SA_FECHADA"},
        {"ano_referencia": 2024, "receita_liquida": 7_800_000_000, "ebitda": 890_000_000,
         "lucro_liquido": 280_000_000, "depreciacao_amort": 145_000_000,
         "fonte_url": "https://www.diario.sc.gov.br/celesc-2024",
         "fonte_tipo": "DIARIO_SC", "fonte_uf": "SC", "confianca_extracao": 0.90},
    ),
    (
        {"cnpj": "05.423.963/0001-11", "razao_social": "Copercampos Cooperativa Agroindustrial",
         "uf": "SC", "municipio": "Campos Novos", "setor": "Agronegócio", "tipo_sociedade": "LTDA"},
        {"ano_referencia": 2024, "receita_liquida": 3_200_000_000, "ebitda": 210_000_000,
         "lucro_liquido": 62_000_000, "depreciacao_amort": 28_000_000,
         "fonte_url": "https://www.diario.sc.gov.br/copercampos-2024",
         "fonte_tipo": "DIARIO_SC", "fonte_uf": "SC", "confianca_extracao": 0.83},
    ),
]


def seed():
    init_db()
    inserted = 0
    for company_data, statement_data in DEMO_COMPANIES:
        try:
            company_id = upsert_company(company_data)
            upsert_statement(company_id, statement_data)
            inserted += 1
            print(f"✅ {company_data['razao_social'][:50]}")
        except Exception as e:
            print(f"⚠️  Erro: {company_data['razao_social'][:40]} — {e}")

    print(f"\n🎯 {inserted} empresas inseridas com sucesso.")


if __name__ == "__main__":
    seed()
