import os
import logging
import json
import uuid
from typing import Optional, Dict, List, Any, Union
import httpx
from hooks.oscar_hook import OscarHook

logger = logging.getLogger(__name__)


class NotifyHook(OscarHook):
    """
    Hook for sending notifications through the middleware's notifier endpoint.

    Expected payload keys:
      - name: (REQUIRED) the notifier name to use.
      - subject: email subject.
      - title: alias for subject. If provided and subject is missing, it is used.
      - message: the body of the email.
      - recipients: either a comma-separated string or a list of email addresses.
    """

    # conn_name_attr = "notify_conn_id"
    # default_conn_name = "notify_default"
    # hook_name = "NotifyHook"

    def __init__(self, conn_id: Optional[str] = None):
        super().__init__(conn_id)
        # Build the notifier send endpoint using base connection settings.
        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/notifiers/send"
        # Build the notifications API endpoint
        self.notifications_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/notifications"
        # Build the notifiers API endpoint for retrieving notifier configurations
        self.notifiers_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/notifiers"
        # Setup headers for internal service authentication
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}

    def send_notification(self, payload: Dict) -> Dict:
        """
        Sends a notification using the middleware notifier service.

        Required payload key:
          - name: the notifier name.

        Additional keys:
          - subject or title: email subject (title used as alias if subject is missing)
          - message: the email body
          - recipients: comma-separated string or list of email addresses
          - attachments: list of attachment objects with filename, content (base64), and content_type

        Returns:
          The JSON-decoded response from the middleware.
        """
        # Ensure the required "name" key is provided.
        if "name" not in payload:
            raise ValueError("Payload must include a 'name' key representing the notifier.")

        # Use 'title' as alias for subject if subject is missing.
        if "subject" not in payload and "title" in payload:
            payload["subject"] = payload["title"]

        # Normalize 'recipients': ensure it's a comma-separated string.
        if "recipients" in payload:
            recips = payload["recipients"]
            if isinstance(recips, list):
                # Join list into a comma-separated string.
                payload["recipients"] = ", ".join([email.strip() for email in recips if email.strip()])
            elif isinstance(recips, str):
                # Clean up any extra whitespace.
                payload["recipients"] = recips.strip()

        # Normalize 'cc_notifier_id': ensure it's a comma-separated string.
        if "cc_notifier_id" in payload:
            cc_notifier_id = payload["cc_notifier_id"]
            if isinstance(cc_notifier_id, list):
                payload["cc_notifier_id"] = ", ".join([email.strip() for email in cc_notifier_id if email.strip()])
            elif isinstance(cc_notifier_id, str):
                payload["cc_notifier_id"] = cc_notifier_id.strip()

        # Normalize 'notifier_id': ensure it's a comma-separated string.
        if "notifier_id" in payload:
            notifier_id = payload["notifier_id"]
            if isinstance(notifier_id, list):
                payload["notifier_id"] = ", ".join([email.strip() for email in notifier_id if email.strip()])
            elif isinstance(notifier_id, str):
                payload["notifier_id"] = notifier_id.strip()

        logger.info(f"Sending notification with payload: {payload}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    self.base_url,
                    params={"notifier_type": "email"},
                    json=payload,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification sent successfully. Response: {response.text}")
                try:
                    return response.json()
                except Exception:
                    return {"response": response.text}
        except Exception as e:
            logger.error(f"Error sending notification: {str(e)}")
            raise e

    def create_notification(self, payload: Dict) -> Dict:
        """
        Creates a new notification using the notification API.

        Required payload keys:
          - user_id: the ID of the user to notify
          - notification_type: the type of notification (e.g., 'EMAIL', 'SMS')
          - subject: the subject of the notification
          - body: the body of the notification

        Optional payload keys:
          - base_template_id: the ID of the base template to use
          - max_reminders: the maximum number of reminders to send
          - expires_at: when the notification expires
          - metadata: additional metadata for the notification

        Returns:
          The JSON-decoded response from the API.
        """
        logger.info(f"Creating notification with payload: {payload}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    self.notifications_url,
                    json=payload,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification created successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error creating notification: {str(e)}")
            raise e

    def get_notification(self, notification_id: Union[str, uuid.UUID]) -> Dict:
        """
        Retrieves a notification by ID.

        Args:
            notification_id: The ID of the notification to retrieve.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        logger.info(f"Retrieving notification with ID: {notification_id}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(
                    f"{self.notifications_url}/{notification_id}",
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification retrieved successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error retrieving notification: {str(e)}")
            raise e

    def update_notification(self, notification_id: Union[str, uuid.UUID], payload: Dict) -> Dict:
        """
        Updates a notification.

        Args:
            notification_id: The ID of the notification to update.
            payload: The update payload.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        logger.info(f"Updating notification {notification_id} with payload: {payload}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.put(
                    f"{self.notifications_url}/{notification_id}",
                    json=payload,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification updated successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error updating notification: {str(e)}")
            raise e

    def cancel_notification(self, notification_id: Union[str, uuid.UUID], reason: Optional[str] = None) -> Dict:
        """
        Cancels a notification.

        Args:
            notification_id: The ID of the notification to cancel.
            reason: Optional reason for cancellation.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        payload = {}
        if reason:
            payload["reason"] = reason

        logger.info(f"Canceling notification {notification_id} with reason: {reason}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.notifications_url}/{notification_id}/cancel",
                    json=payload,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification canceled successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error canceling notification: {str(e)}")
            raise e

    def resend_notification(self, notification_id: Union[str, uuid.UUID], force: bool = False) -> Dict:
        """
        Resends a notification.

        Args:
            notification_id: The ID of the notification to resend.
            force: Whether to force resend even if max reminders reached.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        payload = {"force": force}

        logger.info(f"Resending notification {notification_id} with force={force}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.notifications_url}/{notification_id}/resend",
                    json=payload,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification resent successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error resending notification: {str(e)}")
            raise e

    def respond_notification(self, notification_id: Union[str, uuid.UUID]) -> Dict:
        """
        Marks a notification as responded.

        Args:
            notification_id: The ID of the notification to mark as responded.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        logger.info(f"Marking notification {notification_id} as responded")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.notifications_url}/{notification_id}/respond",
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification marked as responded successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error marking notification as responded: {str(e)}")
            raise e

    def escalate_notification(self, notification_id: Union[str, uuid.UUID]) -> Dict:
        """
        Escalates a notification.

        Args:
            notification_id: The ID of the notification to escalate.

        Returns:
            The JSON-decoded response from the API.
        """
        notification_id = str(notification_id)
        logger.info(f"Escalating notification {notification_id}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.notifications_url}/{notification_id}/escalate",
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notification escalated successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error escalating notification: {str(e)}")
            raise e

    def list_notifications(
        self,
        page: int = 1,
        per_page: int = 10,
        order: str = "desc",
        user_id: Optional[str] = None,
        status: Optional[str] = None,
        notification_type: Optional[str] = None,
        escalated: Optional[bool] = None,
        filter: Optional[str] = None
    ) -> Dict:
        """
        Lists notifications with optional filtering.

        Args:
            page: The page number.
            per_page: The number of items per page.
            order: The sort order ('asc' or 'desc').
            user_id: Filter by user ID.
            status: Filter by status.
            notification_type: Filter by notification type.
            escalated: Filter by escalation status.
            filter: Advanced filter in JSON format.

        Returns:
            The JSON-decoded response from the API.
        """
        params = {
            "page": page,
            "perPage": per_page,
            "order": order
        }

        if user_id:
            params["user_id"] = user_id
        if status:
            params["status"] = status
        if notification_type:
            params["notification_type"] = notification_type
        if escalated is not None:
            params["escalated"] = escalated
        if filter:
            params["filter"] = filter

        logger.info(f"Listing notifications with params: {params}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(
                    self.notifications_url,
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notifications retrieved successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error listing notifications: {str(e)}")
            raise e

    def get_notifier(self, identifier: str, by_name: bool = True) -> Dict:
        """
        Retrieves a notifier by ID or name.

        Args:
            identifier: The ID or name of the notifier to retrieve.
            by_name: Whether to interpret the identifier as a name. Defaults to True.

        Returns:
            The JSON-decoded notifier configuration, which includes:
                - For email notifiers: id, name, type, config (with recipients list), enabled, etc.
                - For webhook notifiers: id, name, type, config (with url), enabled, etc.

        Raises:
            Exception: If the notifier is not found or an error occurs.
        """
        logger.info(f"Retrieving notifier with identifier: {identifier} (by_name={by_name})")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                params = {"by_name": by_name} if by_name else {}
                response = client.get(
                    f"{self.notifiers_url}/{identifier}",
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Notifier retrieved successfully. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error retrieving notifier: {str(e)}")
            raise e
