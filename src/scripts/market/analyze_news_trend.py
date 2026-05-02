
import os
import sys
import logging
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from config.settings import settings

# Initialize logging and console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
console = Console()

def get_llm():
    """Simple LLM factory based on project settings."""
    if settings.llm_base_url:
        return ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
            api_key=settings.openai_api_key or "local",
            temperature=0.2,
        )
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        temperature=0.2,
    )

def fetch_last_5_days_news():
    """Fetch news articles from ClickHouse for the last 5 days."""
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    
    query = """
    SELECT 
        fetched_at,
        category,
        sentiment,
        impact_tier,
        title,
        source,
        url
    FROM market_data.news_articles
    WHERE fetched_at >= now() - INTERVAL 5 DAY
    ORDER BY fetched_at DESC
    """
    
    try:
        result = client.query(query)
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]
        return rows
    except Exception as e:
        logger.error(f"Failed to fetch news from ClickHouse: {e}")
        return []
    finally:
        client.close()

def identify_trend(articles):
    """Analyze articles and identify trends using LLM."""
    if not articles:
        return "No news articles found in the last 5 days."

    # Group articles for the prompt
    news_context = ""
    for idx, art in enumerate(articles[:50]): # Limit to top 50 for context window
        news_context += f"- [{art['category']}] ({art['sentiment']}/{art['impact_tier']}) {art['title']} (Source: {art['source']})\n"

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a professional financial market analyst. Analyze the following news articles from the last 5 days and identify the key market trends, sentiment shifts, and potential risks."),
        ("human", "Recent News Articles:\n{news_context}\n\nPlease provide a concise trend analysis report including:\n1. Overall Market Sentiment\n2. Key Sector Trends\n3. Emerging Risks\n4. Notable Opportunities")
    ])

    llm = get_llm()
    chain = prompt | llm
    
    with console.status("[bold cyan]Analyzing trends with LLM...[/bold cyan]"):
        response = chain.invoke({"news_context": news_context})
        return response.content

def main():
    console.print(Panel("[bold blue]Last 5 Days News Trend Analysis[/bold blue]"))
    
    articles = fetch_last_5_days_news()
    
    if not articles:
        console.print("[yellow]No news articles found in the DB for the last 5 days.[/yellow]")
        console.print("[dim]Tip: Run 'mosaic etf-news --save' to populate the DB with latest news.[/dim]")
        return

    # Display a summary table of retrieved news
    table = Table(title=f"Retrieved {len(articles)} Articles (Last 5 Days)")
    table.add_column("Date", style="cyan")
    table.add_column("Category", style="magenta")
    table.add_column("Sentiment", style="green")
    table.add_column("Title", style="white")

    # Show all in the table
    for art in articles:
        sentiment_color = "green" if art['sentiment'] == 'POSITIVE' else "red" if art['sentiment'] == 'NEGATIVE' else "yellow"
        table.add_row(
            art['fetched_at'].strftime("%Y-%m-%d %H:%M"),
            art['category'],
            f"[{sentiment_color}]{art['sentiment']}[/{sentiment_color}]",
            art['title']
        )
    
    console.print(table)
    
    # Identify and print trend
    if settings.openai_api_key and "your_openai_api_key" not in settings.openai_api_key:
        trend_report = identify_trend(articles)
        console.print(Panel(trend_report, title="[bold green]AI Trend Analysis Report[/bold green]", border_style="green"))
    else:
        console.print("[yellow]Skipping LLM analysis due to missing/placeholder API key.[/yellow]")
        console.print("[cyan]The Gemini CLI agent will analyze the articles above to identify the trend.[/cyan]")

if __name__ == "__main__":
    main()
