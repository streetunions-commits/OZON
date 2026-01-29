#!/usr/bin/env python3
"""
рџљЂ Р“Р»Р°РІРЅС‹Р№ СЃРєСЂРёРїС‚ РґР»СЏ Ozon FBO Tracker
РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРё:
1. РџСЂРѕРІРµСЂСЏРµС‚ РѕР±РЅРѕРІР»РµРЅРёСЏ РЅР° GitHub РєР°Р¶РґСѓСЋ РјРёРЅСѓС‚Сѓ
2. Р•СЃР»Рё РµСЃС‚СЊ РѕР±РЅРѕРІР»РµРЅРёСЏ - СЃРєР°С‡РёРІР°РµС‚ РёС…
3. Flask Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїРµСЂРµР·Р°РіСЂСѓР¶Р°РµС‚ РїСЂРёР»РѕР¶РµРЅРёРµ (debug=True)
"""

import subprocess
import os
import time
import threading
from datetime import datetime

# ============================================================================
# РљРћРќР¤РР“РЈР РђР¦РРЇ
# ============================================================================

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
CHECK_INTERVAL = 60  # РџСЂРѕРІРµСЂСЏРµРј РєР°Р¶РґС‹Рµ 60 СЃРµРєСѓРЅРґ
MAIN_SCRIPT = "ozon_app.py"

# ============================================================================
# Р¤РЈРќРљР¦РР
# ============================================================================

def log(message, prefix=""):
    """Р›РѕРіРёСЂРѕРІР°РЅРёРµ СЃ timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    if prefix:
        print(f"[{ts}] {prefix} {message}")
    else:
        print(f"[{ts}] {message}")

def git_pull_loop():
    """Р¤РѕРЅРѕРІС‹Р№ РїРѕС‚РѕРє РґР»СЏ РїСЂРѕРІРµСЂРєРё РѕР±РЅРѕРІР»РµРЅРёР№"""
    log("рџ”„ Git sync РїРѕС‚РѕРє Р·Р°РїСѓС‰РµРЅ", "рџ”Ђ")
    
    while True:
        try:
            time.sleep(CHECK_INTERVAL)
            
            result = subprocess.run(
                ["git", "pull", "origin", "main"],
                cwd=REPO_PATH,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                if "Already up to date" not in result.stdout and "Already up-to-date" not in result.stdout:
                    log("вњ… РћР‘РќРћР’Р›Р•РќРРЇ РџРћР›РЈР§Р•РќР« РЎ GITHUB!", "рџ”Ђ")
                    log("Flask Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїРµСЂРµР·Р°РіСЂСѓР¶Р°РµС‚ РїСЂРёР»РѕР¶РµРЅРёРµ...", "рџ”Ђ")
                    log("РќР°Р¶РјРёС‚Рµ F5 РІ Р±СЂР°СѓР·РµСЂРµ С‡С‚РѕР±С‹ РѕР±РЅРѕРІРёС‚СЊ СЃС‚СЂР°РЅРёС†Сѓ", "рџ”Ђ")
                else:
                    log("вњ“ РЈР¶Рµ Р°РєС‚СѓР°Р»СЊРЅРѕ", "рџ”Ђ")
            else:
                log(f"вљ пёЏ  Git РѕС€РёР±РєР°: {result.stderr[:100]}", "рџ”Ђ")
                
        except Exception as e:
            log(f"вќЊ РћС€РёР±РєР° РїСЂРё РїСЂРѕРІРµСЂРєРµ РѕР±РЅРѕРІР»РµРЅРёР№: {e}", "рџ”Ђ")

def main():
    """Р“Р»Р°РІРЅР°СЏ С„СѓРЅРєС†РёСЏ"""
    os.chdir(REPO_PATH)
    
    print("\n" + "="*70)
    print("рџљЂ OZON FBO TRACKER - AUTO-UPDATE Р’Р•Р РЎРРЇ")
    print("="*70)
    log(f"рџ“‚ РџР°РїРєР°: {REPO_PATH}", "рџ“Ќ")
    log(f"вЏ° РџСЂРѕРІРµСЂРєР° РѕР±РЅРѕРІР»РµРЅРёР№ РєР°Р¶РґС‹Рµ {CHECK_INTERVAL} СЃРµРє", "вљ™пёЏ")
    print("="*70 + "\n")
    
    # Р—Р°РїСѓСЃРєР°РµРј С„РѕРЅРѕРІС‹Р№ РїРѕС‚РѕРє РґР»СЏ Git СЃРёРЅРєР°
    git_thread = threading.Thread(target=git_pull_loop, daemon=True)
    git_thread.start()
    
    log("рџЊђ Р—Р°РїСѓСЃРєР°СЋ Flask РїСЂРёР»РѕР¶РµРЅРёРµ...", "рџљЂ")
    log("РћС‚РєСЂРѕР№С‚Рµ Р±СЂР°СѓР·РµСЂ: http://localhost:5000", "рџ’»")
    log("", "")
    
    # Р—Р°РїСѓСЃРєР°РµРј РѕСЃРЅРѕРІРЅРѕРµ РїСЂРёР»РѕР¶РµРЅРёРµ
    try:
        subprocess.run(
            ["python", MAIN_SCRIPT],
            cwd=REPO_PATH,
            check=False
        )
    except KeyboardInterrupt:
        log("рџ‘‹ РџСЂРёР»РѕР¶РµРЅРёРµ РѕСЃС‚Р°РЅРѕРІР»РµРЅРѕ", "рџ›‘")
    except Exception as e:
        log(f"вќЊ РћС€РёР±РєР° РїСЂРё Р·Р°РїСѓСЃРєРµ: {e}", "вќЊ")

if __name__ == "__main__":
    main()
