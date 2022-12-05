from transformers import MarianMTModel, MarianTokenizer


model_name = "Helsinki-NLP/opus-mt-en-ROMANCE"
tokenizer = MarianTokenizer.from_pretrained(model_name)
model = MarianMTModel.from_pretrained(model_name)


def translation(src_text):
    translated = model.generate(**tokenizer(src_text, return_tensors="pt", padding=True,max_length = 1024))
    tgt_text = [tokenizer.decode(t, skip_special_tokens=True) for t in translated]
    return tgt_text
            


            

print(translation('Fuck you'))
