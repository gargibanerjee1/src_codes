import streamlit as st
import requests
from TTS.api import TTS
import os
import platform
import dotenv
import os
import json

#load API key from json

with open("config.json") as f:
    config = json.load(f)

api_key = config["API_KEY"]


# Function to fetch headlines
def get_headlines():
    url = f"https://content.guardianapis.com/search?&api-key={api_key}&page-size=20"
    
    response = requests.get(url)
    
    if response.status_code != 200:
        st.error("Failed to retrieve the API data.")
        return []
    
    data = response.json()  # Parse the JSON response
    
    headlines = []
    articles = data.get('response', {}).get('results', [])
    
    for article in articles:
        headline = article.get('webTitle', '')
        if headline:
            headlines.append(headline)
    
    return headlines

# Function to get desktop path based on the OS
def get_desktop_path():
    system_platform = platform.system()
    if system_platform == "Windows":
        return os.path.join(os.path.expanduser('~'), "Desktop")
    else:
        return os.path.join(os.path.expanduser('~'), "Desktop")

# Streamlit UI
st.title("Today's Headlines")

# Fetch headlines from The Guardian
headlines = get_headlines()

if headlines:
    # Display headlines
    st.write("### Latest Headlines:")
    for headline in headlines:
        st.write(f"- {headline}")
    
    # Prepare text for speech synthesis
    text = "Here are today's headlines: " + ". ".join(headlines)

    # Load the TTS model
    tts = TTS("tts_models/en/ljspeech/vits").to("cpu")

    # Define MP3 and TXT file paths for saving to Desktop
    desktop_path = get_desktop_path()
    mp3_file = os.path.join(desktop_path, "news_headlines.mp3")
    txt_file = os.path.join(desktop_path, "headlines.txt")

    # Generate speech and save it to a file
    tts.tts_to_file(text, file_path=mp3_file)
    
    # Play the MP3 file
    st.audio(mp3_file, format="audio/mp3")
    
    # option to download the TXT file
    with open(txt_file, "w") as f:
        f.write("\n".join(headlines))

    st.download_button(
        label="Download Headlines as TXT",
        data=open(txt_file, "rb").read(),
        file_name="headlines.txt",
        mime="text/plain"
    )
    # option to download the MP3 file
    with open(mp3_file, "rb") as mp3:
        st.download_button(
            label="Download MP3",
            data=mp3,
            file_name="news_headlines.mp3",
            mime="audio/mp3"
        )

else:
    st.write("No headlines found.")
