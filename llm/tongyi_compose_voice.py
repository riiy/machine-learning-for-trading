# coding=utf-8
import os
import dashscope
from dashscope.audio.tts import SpeechSynthesizer


dashscope.api_key = 'sk-c7b2b5ce9bba469c96196e6ca365f1b7'


def main(text: str, save_path: str):
    if not text:
        raise
    if os.path.exists(save_path):
        raise
    result = SpeechSynthesizer.call(model='sambert-zhiqi-v1', text=text)
    if result.get_audio_data() is not None:
        with open(save_path, 'wb') as f:
            f.write(result.get_audio_data())


if __name__ == '__main__':
    with open('/home/riiy/org-roam/《悉达多》摘抄.org') as f:
        contents = f.read()
    main(contents, 'output.wav')
