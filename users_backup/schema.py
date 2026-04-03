import strawberry
from typing import Optional
from sqlalchemy.orm import Session
from .models import UserModel


class User:
    id: int
    name: str
    surname:Optional[str]
    expiring_url: Optional[ExpiringUrl]
    profile_picture: str
    instance: strawberry.Private[UserModel]
    designation: str
    employee_id: str
    title: str
    role: str
    role_id: int
    ppm: int
    gender: Optional[str]
    user_character: Optional[str]

    @classmethod
    def from_instance(cls, db:Session,instance: UserModel):
        return cls(instance=instance,
                    id=instance.id,
                    gender=None if instance.gender is None else instance.gender,
                    name=str(instance.name).lower(),
                    surname=str(instance.surname).lower(),
                    expiring_url=None if instance.presigned_url is None else ExpiringUrl(
                        url=instance.presigned_url[0], url_expiry=instance.presigned_url[1]),
                    profile_picture='' if instance.presigned_url is None else instance.presigned_url[
                        0],
                    designation=instance.current_role.name,
                    employee_id = instance.employee_id,
                    title=None if instance.mdesignation.designation is None else instance.mdesignation.designation,
                    role=None if instance.current_role.name is None else instance.current_role.name,
                    ppm=None if instance.mdesignation.ppm is None else instance.mdesignation.ppm,
                    user_character=None if instance.user_character is None else instance.user_character,
                    role_id=instance.current_role_id,