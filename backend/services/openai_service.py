from openai import AzureOpenAI
import logging
import os
import time
import json
from typing import List, Tuple
class OpenAIService:
    @staticmethod
    def get_openai_client() -> AzureOpenAI:
        """
        Create (or return) a singleton AzureOpenAI client using environment variables.
        Can be updated at runtime via /set-openai-config.
        Required env:
        OPENAI_ENDPOINT, OPENAI_API_KEY, OPENAI_API_VERSION, OPENAI_DEPLOYMENT_NAME
        """
        global _openai_client
        if _openai_client is None:
            endpoint   = os.environ.get("OPENAI_ENDPOINT",   "").strip().rstrip("/")
            api_key    = os.environ.get("OPENAI_API_KEY",    "").strip()
            api_version = os.environ.get("OPENAI_API_VERSION", "").strip()

            if not endpoint or not api_key or not api_version:
                raise RuntimeError(
                    "OpenAI configuration missing. "
                    "Set OPENAI_ENDPOINT, OPENAI_API_KEY, OPENAI_API_VERSION (via env or Settings UI)."
                )

            logging.info(
                "Initializing AzureOpenAI client with endpoint=%s api_version=%s",
                endpoint, api_version,
            )

            _openai_client = AzureOpenAI(
                api_version=api_version,
                azure_endpoint=endpoint,
                api_key=api_key,
            )
        return _openai_client
    @staticmethod
    def classify_document(content: str) -> str:
        client     = OpenAIService.get_openai_client()
        deployment = os.environ.get("OPENAI_DEPLOYMENT_NAME", "gpt-35-turbo")
        system_prompt = (
            "You are a classifier for recruitment documents.\n"
            "You receive the full text content of a PDF.\n"
            "Classify it into exactly one of these categories:\n"
            "- JD: A job description / job posting / role description.\n"
            "- CV: A curriculum vitae, résumé, or candidate profile.\n"
            "- OTHER: Anything else (reports, articles, invoices, forms, etc.).\n"
            "Output only the label: JD, CV, or OTHER. No explanation."
        )

        user_prompt = (
            "Document text:\n"
            "```text\n"
            f"{content}\n"
            "```\n"
            "What is the correct label (JD, CV, or OTHER) for this document? "
            "Output only the label."
        )

        try:
            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                max_tokens=5,
                temperature=0.0,
            )
            label = response.choices[0].message.content.strip().upper()
            if label not in ("JD", "CV", "OTHER"):
                logging.warning(
                    "Unexpected classification label '%s', defaulting to OTHER.", label
                )
                return "OTHER"
            return label
        except Exception as e:
            logging.error("Exception during OpenAI classification: %s", e)
            return "OTHER"
        finally:
            time.sleep(2)
    @staticmethod
    def extract_skills_from_jd(jd_text: str) -> List[str]:
        client     =  OpenAIService.get_openai_client()
        deployment = os.environ["OPENAI_DEPLOYMENT_NAME"]
        system_prompt = (
            "You analyze job descriptions and extract the 8 most important required skills.\n"
            "Skills should be short capability phrases, e.g., 'Python programming', "
            "'Project management', 'Stakeholder communication'.\n"
            "Apart from these 8 skills there will be two additional skills.\n"
            "One of them is exactly : 'Bachelors/Masters degree from premier institute'.\n"
            "The other one is exactly: 'Work experience in premier company in decent job role'.\n"
            "Finally, return exactly 10 unique skills in JSON format ONLY, with no extra text, "
            "no markdown, no explanation."
        )

        user_prompt = (
            "Here is the full text of a job description:\n"
            "```text\n"
            f"{jd_text}\n"
            "```\n"
            "Extract the 10 most important required skills for this job.\n"
            "Return JSON only, like:\n"
            "{\n"
            '  \"skills\": [\n'
            '    \"Skill 1\",\n'
            '    \"Skill 2\",\n'
            "    ...\n"
            "  ]\n"
            "}"
        )

        try:
            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                max_tokens=512,
                temperature=0.0,
            )
            raw = response.choices[0].message.content.strip()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                start = raw.find("{")
                end   = raw.rfind("}")
                if start == -1 or end == -1 or end <= start:
                    logging.error(
                        "OpenAI skill extraction response is not valid JSON: %s", raw
                    )
                    return []
                trimmed = raw[start: end + 1]
                try:
                    data = json.loads(trimmed)
                except json.JSONDecodeError as e2:
                    logging.error(
                        "Failed to parse JSON from OpenAI skill extraction response: %s\nTrimmed: %s",
                        e2, trimmed,
                    )
                    return []

            skills = data.get("skills", [])
            skills = [str(s).strip() for s in skills if s]
            if len(skills) > 10:
                skills = skills[:10]
            return skills
        except Exception as e:
            logging.error("Exception during OpenAI skill extraction: %s", e)
            return []
        finally:
            time.sleep(5)

    @staticmethod
    def score_cv_against_skills(
        cv_text: str, skills: List[str]
    ) -> Tuple[List[float], float]:
        client     = OpenAIService.get_openai_client()
        deployment = os.environ["OPENAI_DEPLOYMENT_NAME"]
        skills_list_text = "\n".join(
            f"{i+1}. {skill}" for i, skill in enumerate(skills)
        )

        system_prompt = (
            "You are evaluating a candidate CV against a list of 10 required job skills.\n"
            "For each skill, assign a score from 0 to 10 based on semantic relevance "
            "of the candidate's experience to that skill.\n"
            "Consider synonyms, related tools and responsibilities, and implied knowledge. "
            "Do not rely on exact keyword matching.\n"
            "For skills regarding education insitutes/degrees or job roles/company experience make use of web \n"
            "to detemine the prestige of instituion , degree / company , job role \n"
            "A score of 0 means no evidence; 10 means very strong depth and relevance.\n"
            "Return JSON only."
        )

        user_prompt = (
            "Required skills:\n"
            f"{skills_list_text}\n\n"
            "CV full text:\n"
            "```text\n"
            f"{cv_text}\n"
            "```\n"
            "For each skill, assign a score from 0 to 10 (numbers only), then compute "
            "the average of the 10 scores (0-10).\n"
            "Return JSON only, like:\n"
            "{\n"
            '  \"skills\": [\n'
            '    {\"name\": \"Skill 1\", \"score\": 0-10},\n'
            "    ...\n"
            "  ],\n"
            '  \"total_score\": 0-10\n'
            "}"
        )

        try:
            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                max_tokens=512,
                temperature=0.0,
            )
            raw   = response.choices[0].message.content.strip()
            start = raw.find("{")
            if start > 0:
                raw = raw[start:]
            data        = json.loads(raw)
            skills_data = data.get("skills", [])
            total_score = float(data.get("total_score", 0.0))

            scores_by_name = {}
            for item in skills_data:
                name = str(item.get("name", "")).strip()
                try:
                    score_val = float(item.get("score", 0.0))
                except Exception:
                    score_val = 0.0
                scores_by_name[name] = max(0.0, min(10.0, score_val))

            scores: List[float] = []
            for s in skills:
                scores.append(scores_by_name.get(s, 0.0))

            if total_score <= 0.0 or total_score > 10.0:
                if scores:
                    total_score = sum(scores) / len(scores)
                else:
                    total_score = 0.0

            return scores, total_score
        except Exception as e:
            logging.error("Exception during OpenAI CV scoring: %s", e)
            return [0.0] * len(skills), 0.0
    @staticmethod
    def handle_jd_upload(full_text) -> List[str]:
        skills = OpenAIService.extract_skills_from_jd(full_text)
        if not skills:
            logging.error(
                "No skills extracted from JD '%s'. JD processing will continue, "
                "but results.xlsx may not be correctly initialized."
            )
        return skills
    @staticmethod
    def handle_cv_upload(full_text : str , skills : List[str]):
        scores, total_score = OpenAIService.score_cv_against_skills(full_text, skills)

        if all(s == 0.0 for s in scores) and total_score == 0.0:
            logging.info(
                "CV '%s' received all-zero scores; retrying scoring once."
            )
            scores, total_score = OpenAIService.score_cv_against_skills(full_text, skills)
        return scores , total_score
