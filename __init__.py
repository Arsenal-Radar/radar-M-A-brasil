"""
Motor de coleta — busca demonstrações financeiras em fontes públicas brasileiras.
Cada coletor é independente e pode ser executado separadamente.
"""

import re
import time
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Generator

logger = logging.getLogger(__name__)

# ── Padrões de extração ──────────────────────────────────────────────────────

CNPJ_RE = re.compile(r'\d{2}\.?\d{3}\.?\d{3}[/\\]?\d{4}-?\d{2}')

MONEY_RE = re.compile(
    r'(?:R\$\s*)?(\d{1,3}(?:\.\d{3})+(?:,\d{2})?|\d{4,}(?:,\d{2})?)'
)

COMPANY_SUFFIXES = ["LTDA", "S.A.", "S/A", "SA ", "EIRELI", "S.A", "LIMITADA"]

FINANCIAL_TERMS = [
    "demonstrações financeiras", "demonstrações contábeis",
    "balanço patrimonial", "demonstração do resultado",
    "resultado do exercício", "receita líquida",
    "lucro líquido", "ebitda", "lajida",
]

EBITDA_LABELS = [
    "ebitda", "lajida",
    "resultado antes dos juros",
    "resultado operacional antes",
    "lucro antes de juros, impostos",
]

RECEITA_LABELS = [
    "receita líquida", "receita operacional líquida",
    "receita de vendas", "receita bruta líquida",
    "net revenue",
]

LUCRO_LABELS = [
    "lucro líquido do exercício",
    "resultado líquido do período",
    "lucro (prejuízo) líquido",
    "resultado do exercício",
]

DEPRE_LABELS = [
    "depreciação e amortização",
    "depreciação, amortização",
    "depreciação e amort",
    "d&a",
]

# Estados brasileiros — Diários Oficiais
DIARIOS_URLS = {
    "SP": "https://www.imprensaoficial.com.br/DO/BuscaDO2001Avancada.aspx",
    "RJ": "https://www.ioerj.com.br/portal/modules/conteudoonline/",
    "MG": "https://www.iof.mg.gov.br",
    "RS": "https://www.ioergs.rs.gov.br/online",
    "PR": "https://www.dioe.pr.gov.br",
    "SC": "https://www.diario.sc.gov.br",
    "BA": "https://www.egba.ba.gov.br",
    "GO": "https://www.goias.gov.br/diario-oficial",
    "PE": "https://www.legisweb.com.br/legislacao/?id=diario",
    "CE": "https://www.ceara.gov.br/diario-oficial",
    "AM": "https://www.imprensaoficial.am.gov.br",
    "PA": "https://www.ioepa.com.br",
    "MA": "https://www.stc.ma.gov.br/doe",
    "MS": "https://www.spdo.ms.gov.br",
    "MT": "https://www.iomat.mt.gov.br",
    "ES": "https://ioes.dio.es.gov.br",
    "DF": "https://www.buriti.df.gov.br",
    "RN": "https://www.diof.rn.gov.br",
    "AL": "https://www.imprensaoficial.al.gov.br",
    "PB": "https://www.doe.pb.gov.br",
    "SE": "https://www.se.gov.br/diario",
    "PI": "https://www.diariooficial.pi.gov.br",
    "RO": "https://www.diof.ro.gov.br",
    "TO": "https://www.diariooficial.to.gov.br",
    "AC": "https://www.diario.ac.gov.br",
    "AP": "https://www.diario.ap.gov.br",
    "RR": "https://www.doe.rr.gov.br",
}

# Diário Oficial da União — busca de atos societários (Seção 3)
DOU_SEARCH_URL = "https://www.in.gov.br/consulta/-/buscar/dou"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# ── Funções auxiliares ───────────────────────────────────────────────────────

def clean_money(value_str: str) -> float | None:
    """Converte '1.234.567,89' → 1234567.89"""
    try:
        s = value_str.strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None


def extract_cnpj(text: str) -> str | None:
    m = CNPJ_RE.search(text)
    return m.group(0) if m else None


def extract_company_name(text: str) -> str | None:
    """Encontra nome de empresa (LTDA, S.A., etc.) no texto."""
    for line in text.split("\n")[:40]:
        line = line.strip()
        if len(line) > 8 and any(s in line.upper() for s in COMPANY_SUFFIXES):
            # Limpar caracteres estranhos
            name = re.sub(r'[^\w\s\./\-,&]', '', line).strip()
            if len(name) > 5:
                return name[:200]
    return None


def extract_year(text: str) -> int | None:
    years = re.findall(r'\b(202[3-5])\b', text)
    if not years:
        return None
    from collections import Counter
    return int(Counter(years).most_common(1)[0][0])


def extract_value_after_label(text: str, labels: list[str]) -> float | None:
    """
    Procura um dos labels no texto e extrai o próximo valor monetário
    na mesma linha ou nas 2 linhas seguintes.
    """
    text_lower = text.lower()
    for label in labels:
        pos = text_lower.find(label)
        if pos == -1:
            continue
        # Pegar trecho de ~150 chars após o label
        snippet = text[pos: pos + 150]
        matches = MONEY_RE.findall(snippet)
        for m in matches:
            val = clean_money(m)
            if val and val > 1000:   # Filtrar valores muito pequenos
                return val
    return None


def has_financial_content(text: str) -> bool:
    text_lower = text.lower()
    hits = sum(1 for term in FINANCIAL_TERMS if term in text_lower)
    return hits >= 2


def calculate_confidence(data: dict) -> float:
    score = 0.0
    if data.get("cnpj"):          score += 0.20
    if data.get("company_name"):  score += 0.15
    if data.get("receita_liquida"): score += 0.20
    if data.get("ebitda"):        score += 0.25
    if data.get("lucro_liquido"): score += 0.10
    if data.get("ano_referencia"): score += 0.10
    return round(min(score, 1.0), 3)


def extract_financials(text: str, source_url: str, fonte_tipo: str, uf: str) -> dict | None:
    """
    Extrai todos os dados financeiros de um bloco de texto.
    Retorna None se não houver dados suficientes ou EBITDA < 40M.
    """
    if not has_financial_content(text):
        return None

    receita    = extract_value_after_label(text, RECEITA_LABELS)
    ebitda     = extract_value_after_label(text, EBITDA_LABELS)
    lucro      = extract_value_after_label(text, LUCRO_LABELS)
    depre      = extract_value_after_label(text, DEPRE_LABELS)

    # Se EBITDA não foi achado diretamente, tentar calcular via EBIT + D&A
    if not ebitda and lucro and depre:
        ebitda = lucro + depre   # aproximação

    # Filtro principal: EBITDA > 40 milhões
    if not ebitda or ebitda < 40_000_000:
        return None

    data = {
        "cnpj":           extract_cnpj(text),
        "company_name":   extract_company_name(text),
        "receita_liquida": receita,
        "ebitda":          ebitda,
        "lucro_liquido":   lucro,
        "depreciacao_amort": depre,
        "ano_referencia":  extract_year(text),
        "fonte_url":       source_url,
        "fonte_tipo":      fonte_tipo,
        "fonte_uf":        uf,
    }
    data["confianca_extracao"] = calculate_confidence(data)

    # Descartar se não tiver nome da empresa nem CNPJ
    if not data["cnpj"] and not data["company_name"]:
        return None

    return data


# ── Coletores por fonte ──────────────────────────────────────────────────────

class BaseCollector:
    fonte_tipo: str = "GENERICO"
    delay: float = 2.0    # segundos entre requisições (respeitar servidores públicos)

    def __init__(self, uf: str = "SP"):
        self.uf = uf
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, **kwargs) -> requests.Response | None:
        try:
            resp = self.session.get(url, timeout=20, **kwargs)
            resp.raise_for_status()
            time.sleep(self.delay)
            return resp
        except Exception as e:
            logger.warning(f"[{self.fonte_tipo}/{self.uf}] GET falhou: {url} — {e}")
            return None

    def collect(self) -> Generator[dict, None, None]:
        raise NotImplementedError


class DOUCollector(BaseCollector):
    """
    Coleta do Diário Oficial da União (in.gov.br) — Seção 3 (atos societários).
    Busca termos financeiros via API pública de consulta.
    """
    fonte_tipo = "DOU"
    delay = 3.0

    SEARCH_API = "https://www.in.gov.br/consulta/-/buscar/dou"
    TERMS = ["demonstrações financeiras", "balanço patrimonial", "resultado do exercício"]

    def collect(self) -> Generator[dict, None, None]:
        for term in self.TERMS:
            yield from self._search_term(term)

    def _search_term(self, term: str) -> Generator[dict, None, None]:
        params = {
            "q":        term,
            "s":        "do3",          # Seção 3
            "exactDate": "personalizado",
            "data":     "01/01/2024",
            "dataFim":  datetime.now().strftime("%d/%m/%Y"),
        }
        resp = self._get(self.SEARCH_API, params=params)
        if not resp:
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        results = soup.select(".resultado-dou-item, .search-result, article")

        for item in results:
            link_tag = item.select_one("a[href]")
            if not link_tag:
                continue
            href = link_tag["href"]
            if not href.startswith("http"):
                href = "https://www.in.gov.br" + href

            detail_resp = self._get(href)
            if not detail_resp:
                continue

            text = BeautifulSoup(detail_resp.text, "html.parser").get_text(" ")
            result = extract_financials(text, href, self.fonte_tipo, "BR")
            if result:
                yield result


class DiarioOficialSPCollector(BaseCollector):
    """
    Coleta do Diário Oficial do Estado de São Paulo (imprensaoficial.com.br).
    Usa a busca avançada por termos financeiros.
    """
    fonte_tipo = "DIARIO_SP"
    delay = 3.0
    uf = "SP"

    BASE = "https://www.imprensaoficial.com.br"
    SEARCH = BASE + "/DO/BuscaDO2001Avancada.aspx"

    def collect(self) -> Generator[dict, None, None]:
        for term in ["demonstrações financeiras", "balanço patrimonial LTDA", "resultado exercício S.A."]:
            yield from self._search(term)

    def _search(self, term: str) -> Generator[dict, None, None]:
        params = {
            "np": term,
            "AnoDe": "2024",
            "AnoAte": str(datetime.now().year),
            "Caderno": "Negócios Especiais",
        }
        resp = self._get(self.SEARCH, params=params)
        if not resp:
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.select("a[href*='/DO/']")

        for link in links[:30]:   # Limitar por execução
            href = link.get("href", "")
            if not href.startswith("http"):
                href = self.BASE + href
            detail = self._get(href)
            if not detail:
                continue
            text = BeautifulSoup(detail.text, "html.parser").get_text(" ")
            result = extract_financials(text, href, self.fonte_tipo, "SP")
            if result:
                yield result


class JUCESPCollector(BaseCollector):
    """
    Coleta da Junta Comercial do Estado de São Paulo (JUCESP).
    Documentos de arquivamento de demonstrações contábeis.
    """
    fonte_tipo = "JUCESP"
    delay = 2.5
    uf = "SP"

    BASE = "https://www.jucesponline.sp.gov.br"

    def collect(self) -> Generator[dict, None, None]:
        # A JUCESP expõe consultas públicas de atos arquivados
        search_url = f"{self.BASE}/pesquisa-atos"
        resp = self._get(search_url, params={
            "tipoAto": "demonstracoes-financeiras",
            "anoInicio": "2024",
        })
        if not resp:
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.select("a[href*='ato'], a[href*='documento']")[:20]:
            href = link.get("href", "")
            if not href.startswith("http"):
                href = self.BASE + href
            detail = self._get(href)
            if not detail:
                continue
            text = BeautifulSoup(detail.text, "html.parser").get_text(" ")
            result = extract_financials(text, href, self.fonte_tipo, "SP")
            if result:
                yield result


class GenericDiarioCollector(BaseCollector):
    """
    Coletor genérico para Diários Oficiais estaduais.
    Faz busca por palavras-chave nas páginas de consulta.
    """
    delay = 3.0

    def __init__(self, uf: str):
        super().__init__(uf)
        self.fonte_tipo = f"DIARIO_{uf}"
        self.base_url = DIARIOS_URLS.get(uf, "")

    def collect(self) -> Generator[dict, None, None]:
        if not self.base_url:
            return

        # Tentativa de busca genérica por termo
        for term in ["demonstrações financeiras", "balanço patrimonial"]:
            encoded = requests.utils.quote(term)
            search_url = f"{self.base_url}?q={encoded}&ano=2024"
            resp = self._get(search_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            # Extrair texto da própria página de resultado
            text = soup.get_text(" ")
            if has_financial_content(text):
                result = extract_financials(text, search_url, self.fonte_tipo, self.uf)
                if result:
                    yield result

            # Seguir links de resultado
            for link in soup.select("a[href]")[:15]:
                href = link.get("href", "")
                if not href.startswith("http"):
                    from urllib.parse import urljoin
                    href = urljoin(self.base_url, href)
                if any(kw in link.get_text().lower() for kw in ["balanço", "demonstração", "resultado", "financei"]):
                    detail = self._get(href)
                    if not detail:
                        continue
                    text2 = BeautifulSoup(detail.text, "html.parser").get_text(" ")
                    result = extract_financials(text2, href, self.fonte_tipo, self.uf)
                    if result:
                        yield result


# ── Orquestrador principal ───────────────────────────────────────────────────

COLLECTORS = {
    "DOU (União)":        lambda: DOUCollector(),
    "Diário Oficial SP":  lambda: DiarioOficialSPCollector(),
    "JUCESP (SP)":        lambda: JUCESPCollector(),
    # Estados adicionais via coletor genérico
    **{
        f"Diário {uf}": (lambda u: lambda: GenericDiarioCollector(u))(uf)
        for uf in ["RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE",
                   "AM", "PA", "MA", "MS", "MT", "ES", "DF"]
    }
}


def run_collector(
    name: str,
    progress_callback=None,
    log_callback=None,
) -> dict:
    """
    Executa um coletor específico e salva os resultados no banco.
    Retorna estatísticas da execução.
    """
    from core.database import upsert_company, upsert_statement, log_pipeline_run, init_db
    init_db()

    factory = COLLECTORS.get(name)
    if not factory:
        return {"error": f"Coletor '{name}' não encontrado"}

    collector = factory()
    stats = {
        "docs_found": 0,
        "docs_parsed": 0,
        "empresas_novas": 0,
        "errors": 0,
        "log": [],
    }
    started = datetime.utcnow().isoformat()

    def log(msg: str):
        stats["log"].append(msg)
        if log_callback:
            log_callback(msg)
        logger.info(f"[{name}] {msg}")

    log(f"Iniciando coletor: {name}")

    try:
        for record in collector.collect():
            stats["docs_found"] += 1

            try:
                company_data = {
                    "cnpj":          record.get("cnpj") or f"SEM_CNPJ_{stats['docs_found']}",
                    "razao_social":  record.get("company_name") or "Empresa não identificada",
                    "uf":            record.get("fonte_uf"),
                    "municipio":     None,
                    "setor":         infer_setor(record.get("company_name", "")),
                    "tipo_sociedade": infer_tipo(record.get("company_name", "")),
                }
                company_id = upsert_company(company_data)

                upsert_statement(company_id, {
                    "ano_referencia":    record.get("ano_referencia"),
                    "receita_liquida":   record.get("receita_liquida"),
                    "ebitda":            record.get("ebitda"),
                    "lucro_liquido":     record.get("lucro_liquido"),
                    "depreciacao_amort": record.get("depreciacao_amort"),
                    "fonte_url":         record.get("fonte_url"),
                    "fonte_tipo":        record.get("fonte_tipo"),
                    "fonte_uf":          record.get("fonte_uf"),
                    "confianca_extracao": record.get("confianca_extracao", 0.5),
                })

                stats["docs_parsed"] += 1
                stats["empresas_novas"] += 1

                ebitda_fmt = f"R$ {record['ebitda']/1e6:.1f}M"
                msg = (
                    f"✅ {record.get('company_name', 'N/D')} | "
                    f"EBITDA {ebitda_fmt} | "
                    f"Conf: {record.get('confianca_extracao', 0):.0%}"
                )
                log(msg)

                if progress_callback:
                    progress_callback(stats["docs_found"])

            except Exception as e:
                stats["errors"] += 1
                log(f"⚠️ Erro ao salvar registro: {e}")

    except Exception as e:
        stats["errors"] += 1
        log(f"❌ Erro no coletor: {e}")

    log(f"Finalizado. Encontrados: {stats['docs_found']} | Salvos: {stats['docs_parsed']}")

    log_pipeline_run(
        fonte=name,
        uf=getattr(collector, "uf", "BR"),
        status="done" if stats["errors"] == 0 else "partial",
        docs_found=stats["docs_found"],
        docs_parsed=stats["docs_parsed"],
        empresas_novas=stats["empresas_novas"],
        started_at=started,
        log_text="\n".join(stats["log"]),
    )

    return stats


def infer_setor(company_name: str) -> str | None:
    """Infere setor a partir do nome da empresa (heurística simples)."""
    if not company_name:
        return None
    name = company_name.upper()
    mapping = {
        "Agronegócio":     ["AGRO", "FAZENDA", "GRÃO", "SOJA", "MILHO", "CANA", "PECUÁRIA"],
        "Saúde":           ["SAÚDE", "HOSPITAL", "CLÍNICA", "MÉDIC", "FARMA", "LABORAT"],
        "Tecnologia":      ["TECH", "TECNOLOGIA", "SOFTWARE", "DIGITAL", "DATA", "SISTEMAS"],
        "Varejo":          ["VAREJO", "SUPERMERCADO", "ATACADO", "DISTRIBUID"],
        "Construção":      ["CONSTRU", "ENGENHARIA", "IMÓVEIS", "INCORP"],
        "Indústria":       ["INDUSTRIA", "FABRICAÇÃO", "MANUFATURA", "METALUR"],
        "Logística":       ["LOGÍSTICA", "TRANSPORTE", "FRETE", "CARGA"],
        "Financeiro":      ["FINANCEIRA", "CRÉDITO", "BANCO", "INVEST", "CAPITAL"],
        "Energia":         ["ENERGIA", "ELÉTRIC", "PETRÓ", "GÁS", "SOLAR", "EÓLICA"],
        "Alimentação":     ["ALIMENTOS", "FRIGORÍFICO", "LATICÍNIO", "BEBIDAS"],
        "Educação":        ["EDUCAÇÃO", "COLÉGIO", "ESCOLA", "FACULDADE", "ENSINO"],
    }
    for setor, keywords in mapping.items():
        if any(kw in name for kw in keywords):
            return setor
    return "Outros"


def infer_tipo(company_name: str) -> str | None:
    if not company_name:
        return None
    name = company_name.upper()
    if "LTDA" in name or "LIMITADA" in name:
        return "LTDA"
    if "S.A." in name or " SA " in name or "S/A" in name:
        return "SA_FECHADA"
    if "EIRELI" in name:
        return "EIRELI"
    return None
