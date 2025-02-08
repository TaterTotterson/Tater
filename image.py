# image.py
import os
import requests
import base64
from dotenv import load_dotenv

load_dotenv()

# Get the base URL for the AUTOMATIC1111 API from .env
AUTOMATIC_URL = os.getenv("AUTOMATIC_URL")
if not AUTOMATIC_URL:
    raise Exception("AUTOMATIC_URL environment variable not set.")

def generate_image(prompt: str) -> bytes:
    """
    Generates an image using the text-to-image endpoint.
    
    :param prompt: The text prompt for image generation.
    :return: The generated image as bytes.
    """
    endpoint = f"{AUTOMATIC_URL}/sdapi/v1/txt2img"
    payload = {
        "prompt": prompt,
        # Optional: adjust these parameters as needed:
        "steps": 20,
        "cfg_scale": 7.0,
        "width": 512,
        "height": 512,
    }
    response = requests.post(endpoint, json=payload)
    if response.status_code == 200:
        result = response.json()
        if "images" in result and result["images"]:
            # The API typically returns images as base64 strings.
            image_b64 = result["images"][0]
            try:
                image_bytes = base64.b64decode(image_b64)
                return image_bytes
            except Exception as e:
                raise Exception(f"Failed to decode the image: {e}")
        else:
            raise Exception("No image returned from the AUTOMATIC1111 API.")
    else:
        raise Exception(f"Image generation failed (status {response.status_code}): {response.text}")

def describe_image(attachment_url: str) -> str:
    """
    Downloads an image from a URL and sends it to the API for description.
    
    :param attachment_url: URL of the image to describe.
    :return: The description (caption) returned by the API.
    """
    # First, download the image from the Discord attachment URL.
    image_response = requests.get(attachment_url)
    if image_response.status_code == 200:
        image_bytes = image_response.content
        image_b64 = base64.b64encode(image_bytes).decode('utf-8')
    else:
        raise Exception("Failed to download image from the provided URL.")

    endpoint = f"{AUTOMATIC_URL}/sdapi/v1/describe"
    payload = {
        "image": image_b64,
    }
    response = requests.post(endpoint, json=payload)
    if response.status_code == 200:
        result = response.json()
        if "caption" in result:
            return result["caption"]
        else:
            raise Exception("No caption returned from the AUTOMATIC1111 API.")
    else:
        raise Exception(f"Image description failed (status {response.status_code}): {response.text}")