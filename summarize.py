import os
import argparse
from pathlib import Path
import google.generativeai as genai

def summarize_news(input_path: str, output_path: str, criteria: str):
    # Hent API-nøgle fra systemet
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("FEJL: GEMINI_API_KEY miljøvariabel er ikke sat.")
        print("Kør f.eks.: export GEMINI_API_KEY='din_api_nøgle_her' (Mac/Linux)")
        print("Eller: set GEMINI_API_KEY=din_api_nøgle_her (Windows CMD)")
        return

    # Konfigurer Gemini
    genai.configure(api_key=api_key)

    # Tjek om vi har nyheder at opsummere
    if not Path(input_path).exists():
        print(f"FEJL: Input-filen '{input_path}' findes ikke.")
        print("Kør 'python store_and_latest.py md --out latest.md' først.")
        return

    print(f"Læser artikler fra {input_path}...")
    content = Path(input_path).read_text(encoding="utf-8")

    # Instruktioner til modellen (System Prompt)
    system_instruction = (
        "Du er en professionel og skarp nyhedsredaktør. Din opgave er at læse "
        "de medfølgende seneste nyhedsartikler og skrive et letlæseligt, dagligt nyhedsbrev i Markdown.\n\n"
        f"FOKUS OG KRITERIER:\n{criteria}\n\n"
        "RETNINGSLINJER:\n"
        "1. Ignorer alle artikler, der ikke er relevante for kriterierne ovenfor.\n"
        "2. For hver relevant artikel: Skriv en fængende overskrift, et kort resumé i 2-3 bullet points, "
        "og inkluder altid kildelinket.\n"
        "3. Inddel i overordnede kategorier, hvis der er mange artikler.\n"
        "4. Skriv på fejlfrit, professionelt dansk."
    )

    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        system_instruction=system_instruction
    )

    print("Sender data til Gemini API (dette kan tage et øjeblik)...")
    try:
        response = model.generate_content(content)
        Path(output_path).write_text(response.text, encoding="utf-8")
        print(f"\nSucces! Dit færdige nyhedsbrev er gemt i: {output_path}")
    except Exception as e:
        print(f"Der opstod en fejl under kommunikationen med Gemini: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Opsummer nyheder med Gemini API")
    parser.add_argument("--input", default="latest.md", help="Sti til input markdown fil")
    parser.add_argument("--out", default="briefing.md", help="Sti til den færdige opsummering")
    parser.add_argument("--criteria", default="Fokuser på nyheder om kunstig intelligens, mediebranchens udvikling, digital journalistik og nye tech-trends.", help="Hvad LLM'en skal fokusere på")
    
    args = parser.parse_args()
    summarize_news(args.input, args.out, args.criteria)