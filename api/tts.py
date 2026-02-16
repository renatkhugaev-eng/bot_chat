"""
ElevenLabs Text-to-Speech API для голосовых сообщений тёти Розы.
Генерирует аудио с "пьяным" голосом.
"""

from http.server import BaseHTTPRequestHandler
import json
import urllib.request
import urllib.error
import os
import base64

# ElevenLabs API
ELEVENLABS_API_KEY = os.environ.get('ELEVENLABS_API_KEY', '')
MAX_CONTENT_LENGTH = 50 * 1024  # 50 KB - защита от DoS (текст не должен быть большим)
ELEVENLABS_API_URL = "https://api.elevenlabs.io/v1/text-to-speech"

# Голос для тёти Розы (можно поменять на другой)
# Популярные женские голоса:
# - "21m00Tcm4TlvDq8ikWAM" - Rachel (американский)
# - "EXAVITQu4vr4xnSDxMaL" - Bella (американский)  
# - "pNInz6obpgDQGcFmaJgB" - Adam (мужской)
# Лучше найти русский или хриплый голос в библиотеке
DEFAULT_VOICE_ID = "21m00Tcm4TlvDq8ikWAM"  # Rachel - потом заменим на лучший

# Настройки для "пьяного" голоса
VOICE_SETTINGS = {
    "stability": 0.25,           # Низкая стабильность = "плывущий" голос
    "similarity_boost": 0.75,    # Сохраняем характер голоса
    "style": 0.85,               # Высокая экспрессивность
    "use_speaker_boost": True    # Улучшение качества
}


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: dict):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _send_audio(self, audio_data: bytes):
        """Отправляем аудио как base64"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        # Кодируем в base64 для передачи
        audio_base64 = base64.b64encode(audio_data).decode('utf-8')
        self.wfile.write(json.dumps({
            "audio": audio_base64,
            "format": "mp3"
        }).encode('utf-8'))
    
    def _send_error(self, status: int, message: str):
        self._send_json(status, {"error": message})
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        if not ELEVENLABS_API_KEY:
            self._send_error(500, "ELEVENLABS_API_KEY not configured")
            return
        
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            
            # Защита от слишком больших запросов
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request body too large")
                return
            
            if content_length == 0:
                self._send_error(400, "Empty request body")
                return
            
            body = self.rfile.read(content_length)
            
            try:
                data = json.loads(body.decode('utf-8'))
            except json.JSONDecodeError as e:
                self._send_error(400, f"Invalid JSON: {str(e)}")
                return
            
            text = data.get('text', '').strip()
            voice_id = data.get('voice_id', DEFAULT_VOICE_ID)
            
            if not text:
                self._send_error(400, "Text is required")
                return
            
            # Ограничиваем длину текста (ElevenLabs лимит)
            if len(text) > 1000:
                text = text[:1000]
            
            # Формируем запрос к ElevenLabs
            url = f"{ELEVENLABS_API_URL}/{voice_id}"
            
            request_body = json.dumps({
                "text": text,
                "model_id": "eleven_multilingual_v2",  # Поддерживает русский!
                "voice_settings": VOICE_SETTINGS
            }).encode('utf-8')
            
            req = urllib.request.Request(
                url,
                data=request_body,
                headers={
                    'Content-Type': 'application/json',
                    'xi-api-key': ELEVENLABS_API_KEY,
                    'Accept': 'audio/mpeg'
                },
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                audio_data = response.read()
            
            # Возвращаем аудио в base64
            self._send_audio(audio_data)
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(e.code, f"ElevenLabs error: {error_body}")
            
        except Exception as e:
            self._send_error(500, f"Error: {str(e)}")
    
    def do_GET(self):
        """GET для проверки и получения списка голосов"""
        if not ELEVENLABS_API_KEY:
            self._send_error(500, "ELEVENLABS_API_KEY not configured")
            return
        
        try:
            # Получаем список доступных голосов
            req = urllib.request.Request(
                "https://api.elevenlabs.io/v1/voices",
                headers={
                    'xi-api-key': ELEVENLABS_API_KEY
                },
                method='GET'
            )
            
            with urllib.request.urlopen(req, timeout=10) as response:
                voices_data = json.loads(response.read().decode('utf-8'))
            
            # Фильтруем только нужную инфу
            voices = []
            for voice in voices_data.get('voices', []):
                voices.append({
                    "voice_id": voice.get('voice_id'),
                    "name": voice.get('name'),
                    "category": voice.get('category'),
                    "labels": voice.get('labels', {})
                })
            
            self._send_json(200, {
                "voices": voices,
                "default_voice_id": DEFAULT_VOICE_ID,
                "current_settings": VOICE_SETTINGS
            })
            
        except Exception as e:
            self._send_error(500, f"Error getting voices: {str(e)}")
