def summarize_error_msg(error_msg, max_len=200):
    """仅保留主要错误信息，避免把完整堆栈写入任务表。"""
    if error_msg is None:
        return None
    text = str(error_msg).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None
    major = text.split("\n", 1)[0].strip()
    return major[:max_len]


class RetryableProcessError(Exception):
    """表示短暂性错误，可重试。"""
