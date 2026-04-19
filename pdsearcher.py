import re

class PDSearcher:
    def __init__(self):
        self.rules = {
            "name": {
                "pattern": r'[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ](?:\.|\s*[А-ЯЁ]\.|[а-яё]+))(?:\s+[А-ЯЁ](?:\.|\s*[а-яё]+))?',
                "validator": None
            },
            "bank_card": {
                "pattern": r'\b(?:\d[ -]*?){13,16}\b',
                "validator": self._validate_bank_card
            },
            "email": {
                "pattern": r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+',
                "validator": None
            },
            "phone": {
                "pattern": r'(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                "validator": None
            },
            "snils": {
                "pattern": r'\b\d{3}-\d{3}-\d{3}\s\d{2}\b',
                "validator": self._validate_snils
            },
            "inn": {
                "pattern": r'\b\d{10}\b|\b\d{12}\b',
                "validator": self._validate_inn
            },
            "passport": {
                "pattern": r'\b\d{2}[\s\-]?\d{2}[\s\-]?\d{6}\b',
                "validator": None
            }
        }
        
    def search(self, text: str) -> dict:
        """Проход по всем правилам."""
        findings = {}
        for name, rule in self.rules.items():
            count = 0
            validator = rule.get("validator")
            for match in re.finditer(rule["pattern"], text):
                value = match.group(0)
                if not validator or validator(value):
                    count += 1
            if count > 0:
                findings[name] = count
        return findings

    def _validate_bank_card(self, card_str):
        digits = [int(d) for d in card_str if d.isdigit()]
        if len(digits) not in (15, 16):
            return False
        total = 0
        is_second = False
        for digit in reversed(digits):
            if is_second:
                digit *= 2
                if digit > 9:
                    digit -= 9
            total += digit
            is_second = not is_second
        
        return total % 10 == 0
    
    def _validate_snils(self, snils_str):
        digits = [int(d) for d in snils_str if d.isdigit()]
        if len(digits) != 11:
            return False
        control = digits[-2] * 10 + digits[-1]
        total = 0
        for i in range(9):
            total += digits[i] * (9 - i)
        if total < 100:
            return total == control
        elif total == 100 or total == 101:
            return control == 0
        else:
            return (total % 101) == control

    def _validate_inn(self, inn_str):
        return len(inn_str) == 10 and self._validate_inn_legal(inn_str) or \
            len(inn_str) == 12 and self._validate_inn_individual(inn_str)

    def _validate_inn_legal(self, inn_str):
        digits = [int(d) for d in inn_str if d.isdigit()]
        if len(digits) != 10:
            return False
        coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8]
        total = sum(digits[i] * coeffs[i] for i in range(9))
        control = total % 11 % 10
        return control == digits[9]

    def _validate_inn_individual(self, inn_str):
        digits = [int(d) for d in inn_str if d.isdigit()]
        if len(digits) != 12:
            return False
        coeffs1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        total1 = sum(digits[i] * coeffs1[i] for i in range(10))
        control1 = total1 % 11 % 10
        coeffs2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        total2 = sum(digits[i] * coeffs2[i] for i in range(11))
        control2 = total2 % 11 % 10        
        return control1 == digits[10] and control2 == digits[11]