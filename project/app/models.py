from pydantic import BaseModel, field_validator, ValidationError


class UserScoreRequest(BaseModel):
    user_id: int
    debt: float

    @field_validator('user_id')
    def validate_user_id(cls, value: int) -> int:
        if value < 100:
            raise ValueError('User ID must be greater than 100', '')

        return value

    @field_validator('debt')
    def validate_debt(cls, value: float) -> float:
        if value < 1000:
            raise ValueError('Debt must be greater than 1000', '')

        return value
