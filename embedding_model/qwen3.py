import torch
import torch.nn.functional as F
from torch import Tensor
from transformers import AutoTokenizer, AutoModel

# ✅ 定义官方 recommended pooling
def last_token_pool(last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
    left_padding = (attention_mask[:, -1].sum() == attention_mask.shape[0])
    if left_padding:
        return last_hidden_states[:, -1]
    else:
        sequence_lengths = attention_mask.sum(dim=1) - 1
        batch_size = last_hidden_states.shape[0]
        return last_hidden_states[torch.arange(batch_size, device=last_hidden_states.device), sequence_lengths]

# ✅ 本地模型路径
model_path = r"E:\model-al\model\Qwen3-Embedding-0.6B"  # ⬅️ 改成你的路径
# model_path = r"E:\model-al\model\Qwen3-Embedding-8B"  # ⬅️ 改成你的路径
tokenizer = AutoTokenizer.from_pretrained(model_path, padding_side="left")
model = AutoModel.from_pretrained(model_path)
# 模型会被放到 GPU
# model = AutoModel.from_pretrained(model_path,torch_dtype=torch.float16).cuda()
print(model.device)
# ✅ 转换向量函数
def get_embedding(text: str) -> list[float]:
    """
    使用 Qwen3 Embedding 官方推荐写法生成向量
    """
    batch_dict = tokenizer(
        text,
        padding=True,
        truncation=True,
        max_length=8192,
        return_tensors="pt",
    )
    batch_dict.to(model.device)

    with torch.no_grad():
        outputs = model(**batch_dict)
        pooled_embeddings = last_token_pool(outputs.last_hidden_state, batch_dict["attention_mask"])
        normalized_embeddings = F.normalize(pooled_embeddings, p=2, dim=1)
        vector = normalized_embeddings[0].cpu().numpy()
        return vector.tolist()
