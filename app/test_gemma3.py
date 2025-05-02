from transformers import AutoTokenizer, Gemma3ForCausalLM # Changed import
import torch
import os

# Asegúrate de que la variable de entorno HF_TOKEN esté configurada
# Esto es necesario para autenticar y descargar el modelo desde Hugging Face
# Puedes configurarla ejecutando: export HF_TOKEN='tu_token_aqui' en tu terminal

# Identificador del modelo en Hugging Face
#model_id = "google/gemma-3-12b-it"
model_id = "google/gemma-3-1b-it"

# Cargar el modelo para CPU
model = Gemma3ForCausalLM.from_pretrained( # Changed class
    model_id,
    # device_map="auto", # Removed for CPU
    torch_dtype=torch.float32 # Changed to float32 for CPU
).eval()

# Cargar el tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_id) # Changed class and variable name

# Ejemplo de mensaje
messages = [
    {
        "role": "system",
        "content": "You are a very helpful assistant." # Simplified content structure for text-only
    },
    {
        "role": "user",
        # Combined the two user messages
        "content": "Can you summarize this paragraph?\nParagraph: The quick brown fox jumps over the lazy dog."
    }
]

# Prepara la entrada utilizando la plantilla de chat
inputs = tokenizer.apply_chat_template( # Changed variable name
    messages,
    add_generation_prompt=True,
    tokenize=True,
    return_dict=True,
    return_tensors="pt"
)

# Asegúrate de mover los tensores al dispositivo del modelo (CPU en este caso)
inputs = inputs.to("cpu") # Removed dtype=torch.float32 as input_ids should be Long

with torch.inference_mode():
    # Genera la respuesta
    # Pass only input_ids and attention_mask if available
    input_ids = inputs["input_ids"]
    attention_mask = inputs.get("attention_mask") # Get attention_mask if present
    generation = model.generate(
        input_ids=input_ids,
        attention_mask=attention_mask, # Pass attention_mask
        max_new_tokens=100,
        do_sample=False,
        top_p=None, # Added to suppress warning
        top_k=None  # Added to suppress warning
    )

# Decodifica la salida para obtener el texto final
# Use input_ids shape directly
decoded = tokenizer.decode(generation[0][input_ids.shape[-1]:], skip_special_tokens=True) # Changed variable name and input reference
print("Respuesta:", decoded)

# --- Ejemplo de análisis de sentimientos ---
print("\n--- Análisis de Sentimientos ---")

# Mensaje para análisis de sentimientos
sentiment_messages = [
    {
        "role": "system",
        "content": "You are a sentiment analysis assistant. Analyze the sentiment of the following text and return the result as a JSON object with a single key 'sentiment' and possible values 'positive', 'negative', or 'neutral'."
    },
    {
        "role": "user",
        "content": "Text to analyze: I had a wonderful time at the party! The music was great and the food was delicious."
    }
]

# Prepara la entrada para análisis de sentimientos
sentiment_inputs = tokenizer.apply_chat_template(
    sentiment_messages,
    add_generation_prompt=True,
    tokenize=True,
    return_dict=True,
    return_tensors="pt"
).to("cpu")

# Genera la respuesta de análisis de sentimientos
with torch.inference_mode():
    sentiment_input_ids = sentiment_inputs["input_ids"]
    sentiment_attention_mask = sentiment_inputs.get("attention_mask")
    sentiment_generation = model.generate(
        input_ids=sentiment_input_ids,
        attention_mask=sentiment_attention_mask,
        max_new_tokens=50, # Shorter max tokens for JSON output
        do_sample=False,
        top_p=None,
        top_k=None
    )

# Decodifica la salida del análisis de sentimientos
sentiment_decoded = tokenizer.decode(sentiment_generation[0][sentiment_input_ids.shape[-1]:], skip_special_tokens=True)
print("Resultado Análisis de Sentimientos:", sentiment_decoded)
