import os
import json
import time
from zhipuai import ZhipuAI

# 初始化智谱AI客户端
client = ZhipuAI(api_key="api key")  # 请替换为你的实际API Key


def find_all_json_files(input_dir):
    """
    递归查找所有JSON文件
    """
    json_files = []
    print(f"🔍 开始在目录中搜索JSON文件: {input_dir}")

    # 使用os.walk递归遍历所有子目录
    for root, dirs, files in os.walk(input_dir):
        json_files_in_dir = [f for f in files if f.endswith('.json')]
        for file in json_files_in_dir:
            full_path = os.path.join(root, file)
            json_files.append(full_path)

        # 每处理一个目录就打印进度
        if json_files_in_dir:
            print(f"  在 {root} 中找到 {len(json_files_in_dir)} 个JSON文件")

    print(f"✅ 总共找到 {len(json_files)} 个JSON文件")
    return json_files


def create_batch_jsonl_with_formula_types(input_dir, output_dir, max_requests_per_file=50000, max_file_size_mb=100):
    """
    创建多个Batch API所需的JSONL文件，自动分片
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 递归查找所有JSON文件
    all_json_files = find_all_json_files(input_dir)

    if not all_json_files:
        print("❌ 没有找到任何JSON文件，请检查路径和文件格式")
        return [], 0

    # 定义简化的公式类型分类（四大类）
    formula_types = {
        "closed_form": "通项公式 (closed form)",
        "recurrence": "递推公式 (recurrence relation)",
        "generating_function": "生成函数 (generating function)",
        "other": "其他类型 (other)"
    }

    file_index = 1
    current_requests = 0
    current_size = 0
    total_requests = 0

    # 创建第一个JSONL文件
    jsonl_file_path = os.path.join(output_dir, f"batch_requests_{file_index}.jsonl")
    f_out = open(jsonl_file_path, 'w', encoding='utf-8')
    jsonl_files = [jsonl_file_path]

    print(f"📝 开始创建JSONL文件: {jsonl_file_path}")

    # 遍历所有找到的JSON文件
    for i, json_file_path in enumerate(all_json_files, 1):
        print(f"🔍 处理文件 ({i}/{len(all_json_files)}): {os.path.basename(json_file_path)}")

        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                seq_data = json.load(f)
        except Exception as e:
            print(f"  ❌ 读取文件时出错: {e}")
            continue

        # 检查必要字段
        if not all(key in seq_data for key in ['sequence_id', 'formulas']):
            print(f"  ⚠️ 文件缺少必要字段，跳过")
            continue

        if not isinstance(seq_data['formulas'], list) or len(seq_data['formulas']) == 0:
            print(f"  ⚠️ formulas字段为空或不是列表，跳过")
            continue

        print(f"  ✅ 序列ID: {seq_data['sequence_id']}, 公式数量: {len(seq_data['formulas'])}")

        # 构建system prompt - 使用简化的四大类分类
        system_prompt = f"""你是一个专业的数学公式解析器。你的任务是从用户提供的文本中精确识别和提取所有数学公式，并对每个公式进行分类。

请将公式分类为以下四种类型之一：
{json.dumps(formula_types, indent=2, ensure_ascii=False)}

分类指南：
1. 通项公式 (closed_form): 直接给出第n项的表达式，如 F(n) = φ^n/√5 - (1-φ)^n/√5
2. 递推公式 (recurrence): 描述项与项之间关系的公式，如 F(n) = F(n-1) + F(n-2)
3. 生成函数 (generating_function): 以幂级数形式表示序列的函数，如 G.f.: x/(1-x-x^2)
4. 其他 (other): 不属于以上三类的任何公式，如矩阵形式、恒等式、连分数等

最终输出请使用JSON格式，包含以下字段：
- "sequence_id": 序列ID
- "extracted_formulas": 列表，每个元素是一个对象，包含:
  - "formula_text": 原始公式文本
  - "formula_type": 公式类型（必须从上述四种类型中选择）
  - "formula_latex": 公式的LaTeX表示（如果适用）
  - "confidence": 你对分类的置信度（0-1之间的数值）

请确保提取和分类尽可能准确。对于不确定的类型，请选择"other"。"""

        # 构建用户消息
        user_prompt = f"Sequence ID: {seq_data['sequence_id']}\nFormulas to classify:\n" + "\n".join(
            [f"{i + 1}. {formula}" for i, formula in enumerate(seq_data['formulas'])])

        # 构造请求体
        request_body = {
            "custom_id": f"request-{total_requests}-{seq_data['sequence_id']}",
            "method": "POST",
            "url": "/v4/chat/completions",
            "body": {
                "model": "glm-4-flash",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1,
                "max_tokens": 2000,
                "response_format": {"type": "json_object"}
            }
        }

        # 转换为JSON字符串并计算大小
        request_json = json.dumps(request_body, ensure_ascii=False)
        request_size = len(request_json.encode('utf-8'))

        # 检查是否需要创建新文件
        if (current_requests >= max_requests_per_file or
                (current_size + request_size) > max_file_size_mb * 1024 * 1024):
            f_out.close()
            print(f"✅ 已创建: {jsonl_file_path} (包含 {current_requests} 个请求, {current_size / 1024 / 1024:.2f} MB)")

            # 创建新文件
            file_index += 1
            current_requests = 0
            current_size = 0
            jsonl_file_path = os.path.join(output_dir, f"batch_requests_{file_index}.jsonl")
            f_out = open(jsonl_file_path, 'w', encoding='utf-8')
            jsonl_files.append(jsonl_file_path)
            print(f"📝 开始创建新文件: {jsonl_file_path}")

        # 写入请求
        f_out.write(request_json + '\n')
        current_requests += 1
        current_size += request_size
        total_requests += 1

        if total_requests % 100 == 0:
            print(f"📊 已处理 {total_requests} 个请求")

    # 关闭最后一个文件
    f_out.close()
    print(f"✅ 已创建: {jsonl_file_path} (包含 {current_requests} 个请求, {current_size / 1024 / 1024:.2f} MB)")
    print(f"📊 总共创建 {len(jsonl_files)} 个JSONL文件，包含 {total_requests} 个请求")

    return jsonl_files, total_requests


def validate_jsonl_file(file_path):
    """验证JSONL文件格式"""
    print(f"🔍 验证JSONL文件: {file_path}")
    line_count = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    # 检查必需字段
                    if not all(key in data for key in ["custom_id", "method", "url", "body"]):
                        print(f"❌ 第 {i} 行缺少必需字段")
                        return False
                    line_count += 1
                except json.JSONDecodeError as e:
                    print(f"❌ 第 {i} 行JSON格式错误: {e}")
                    return False
    except Exception as e:
        print(f"❌ 读取文件时出错: {e}")
        return False

    print(f"✅ JSONL文件验证通过，包含 {line_count} 个有效请求")
    return line_count > 0  # 确保文件不为空


def submit_batch_task_with_retry(jsonl_file_path, max_retries=3):
    """带重试机制的任务提交"""
    for attempt in range(max_retries):
        try:
            print(f"🔄 尝试 {attempt + 1}/{max_retries}: 上传文件 {os.path.basename(jsonl_file_path)}")

            # 上传文件
            with open(jsonl_file_path, "rb") as f:
                upload_result = client.files.create(file=f, purpose="batch")
            file_id = upload_result.id
            print(f"  ✅ 文件上传成功，ID: {file_id}")

            # 创建Batch任务
            batch_create_result = client.batches.create(
                input_file_id=file_id,
                endpoint="/v4/chat/completions",
                completion_window="24h",
                metadata={
                    "description": "OEIS公式分类任务（四大类）",
                    "original_filename": os.path.basename(jsonl_file_path)
                }
            )

            batch_id = batch_create_result.id
            print(f"  ✅ Batch任务创建成功，ID: {batch_id}")
            return batch_id

        except Exception as e:
            print(f"  ❌ 尝试 {attempt + 1} 失败: {e}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)  # 递增等待时间
                print(f"  ⏳ 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)

    return None


def submit_batch_tasks(jsonl_files, task_id_file):
    """提交多个批量任务并保存所有任务ID"""
    task_ids = []
    successful_files = 0

    for i, jsonl_file_path in enumerate(jsonl_files, 1):
        print(f"\n📋 处理文件 {i}/{len(jsonl_files)}: {os.path.basename(jsonl_file_path)}")

        # 验证文件格式
        if not validate_jsonl_file(jsonl_file_path):
            print(f"  ⚠️ 文件验证失败，跳过: {jsonl_file_path}")
            continue

        # 提交任务（带重试）
        batch_id = submit_batch_task_with_retry(jsonl_file_path)

        if batch_id:
            task_ids.append(batch_id)
            successful_files += 1
            print(f"  ✅ 成功创建任务: {batch_id}")
        else:
            print(f"  ❌ 所有重试均失败: {jsonl_file_path}")

    # 保存所有任务ID到文件
    if task_ids:
        with open(task_id_file, 'w') as f:
            for task_id in task_ids:
                f.write(task_id + '\n')

        print(f"\n📝 成功创建 {successful_files} 个任务，ID已保存到: {task_id_file}")
    else:
        print("\n❌ 未能创建任何任务")

    return task_ids


if __name__ == "__main__":
    # 配置路径
    input_directory = "D:/nn/oeis_onlyclean_json"  # 你的JSON文件目录
    output_directory = "batch_requests2"  # 输出JSONL文件的目录
    task_id_file = "batch_task_ids2.txt"  # 保存任务ID的文件

    print("🚀 开始Batch任务提交流程...")

    # 1. 创建JSONL请求文件
    print("\n" + "=" * 50)
    print("步骤1: 创建JSONL请求文件")
    print("=" * 50)
    jsonl_files, total_requests = create_batch_jsonl_with_formula_types(
        input_directory,
        output_directory,
        max_requests_per_file=50000,  # 每个文件最多50,000个请求
        max_file_size_mb=100  # 每个文件最大100MB
    )

    if total_requests == 0:
        print("❌ 没有创建任何请求，请检查JSON文件格式和内容")
        exit(1)

    # 2. 提交所有任务
    print("\n" + "=" * 50)
    print("步骤2: 提交Batch任务")
    print("=" * 50)
    task_ids = submit_batch_tasks(jsonl_files, task_id_file)

    print("\n" + "=" * 50)
    print("任务提交摘要")
    print("=" * 50)
    print(f"• 创建的JSONL文件数: {len(jsonl_files)}")
    print(f"• 总请求数: {total_requests}")
    print(f"• 成功创建的任务数: {len(task_ids)}")
    print(f"• 任务ID文件: {task_id_file}")

    if task_ids:
        print("\n🎉 任务提交完成！")
    else:

        print("\n❌ 任务提交失败，请检查错误信息。")
