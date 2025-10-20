"""
Blimp Cronjob Scheduler Server
Runs scheduled workflows at specified times by calling the MCP server endpoints.
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


class WorkflowScheduler:
    """Handles scheduling and execution of workflows"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.http_client = httpx.AsyncClient(timeout=300.0)  # 5 minute timeout
        
    async def fetch_scheduled_workflows(self) -> List[Dict[str, Any]]:
        """Fetch all active scheduled workflows from database"""
        try:
            response = supabase.table("scheduled_workflows").select("*").eq("is_active", True).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error fetching scheduled workflows: {e}")
            return []
    
    async def should_execute_workflow(self, workflow: Dict[str, Any]) -> bool:
        """
        Determine if a workflow should be executed based on its schedule
        
        Args:
            workflow: Scheduled workflow object with schedule_type, schedule_config, last_run
        
        Returns:
            True if workflow should execute now, False otherwise
        """
        now = datetime.utcnow()
        schedule_type = workflow.get("schedule_type")  # 'daily', 'weekly', 'monthly', 'custom'
        schedule_config = workflow.get("schedule_config", {})  # JSON with schedule details
        last_run = workflow.get("last_run")
        
        # Parse last_run if it exists
        if last_run:
            last_run_dt = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
        else:
            last_run_dt = None
        
        # Check based on schedule type
        if schedule_type == "daily":
            # Run daily at specified hour
            target_hour = schedule_config.get("hour", 0)
            target_minute = schedule_config.get("minute", 0)
            
            # Check if current time matches target time
            if now.hour == target_hour and now.minute == target_minute:
                # Check if already run today
                if last_run_dt and last_run_dt.date() == now.date():
                    return False
                return True
                
        elif schedule_type == "weekly":
            # Run weekly on specified day and time
            target_day = schedule_config.get("day_of_week", 0)  # 0=Monday, 6=Sunday
            target_hour = schedule_config.get("hour", 0)
            target_minute = schedule_config.get("minute", 0)
            
            if now.weekday() == target_day and now.hour == target_hour and now.minute == target_minute:
                # Check if already run this week
                if last_run_dt:
                    days_since_last_run = (now - last_run_dt).days
                    if days_since_last_run < 7:
                        return False
                return True
                
        elif schedule_type == "monthly":
            # Run monthly on specified day and time
            target_day = schedule_config.get("day_of_month", 1)
            target_hour = schedule_config.get("hour", 0)
            target_minute = schedule_config.get("minute", 0)
            
            if now.day == target_day and now.hour == target_hour and now.minute == target_minute:
                # Check if already run this month
                if last_run_dt and last_run_dt.month == now.month and last_run_dt.year == now.year:
                    return False
                return True
                
        elif schedule_type == "custom":
            # Custom cron-like schedule
            # schedule_config should have: hour, minute, day_of_week (optional), day_of_month (optional)
            target_hour = schedule_config.get("hour")
            target_minute = schedule_config.get("minute")
            target_day_of_week = schedule_config.get("day_of_week")  # Can be None
            target_day_of_month = schedule_config.get("day_of_month")  # Can be None
            
            # Check time match
            time_matches = (target_hour is None or now.hour == target_hour) and \
                          (target_minute is None or now.minute == target_minute)
            
            # Check day match
            day_matches = True
            if target_day_of_week is not None:
                day_matches = day_matches and (now.weekday() == target_day_of_week)
            if target_day_of_month is not None:
                day_matches = day_matches and (now.day == target_day_of_month)
            
            if time_matches and day_matches:
                # Check if already run in the last hour to prevent duplicates
                if last_run_dt:
                    minutes_since_last_run = (now - last_run_dt).total_seconds() / 60
                    if minutes_since_last_run < 60:
                        return False
                return True
        
        return False
    
    async def execute_workflow(self, scheduled_workflow: Dict[str, Any]):
        """
        Execute a scheduled workflow by calling the appropriate MCP server endpoint
        
        Args:
            scheduled_workflow: Scheduled workflow object from database
        """
        workflow_id = scheduled_workflow.get("id")
        user_id = scheduled_workflow.get("user_id")
        workflow_data = scheduled_workflow.get("workflow_data", {})
        is_custom = scheduled_workflow.get("is_custom_workflow", False)
        
        logger.info(f"Executing scheduled workflow {workflow_id} for user {user_id}")
        
        try:
            # Determine which endpoint to call
            if is_custom:
                endpoint = f"{MCP_SERVER_URL}/api/execute-custom-workflow"
                payload = {
                    "user_id": user_id,
                    "workflow_title": workflow_data.get("workflow_title", "Scheduled Workflow"),
                    "workflow_json": workflow_data.get("workflow_json", {})
                }
            else:
                endpoint = f"{MCP_SERVER_URL}/api/execute-workflow"
                payload = {
                    "user_id": user_id,
                    "workflow_id": workflow_data.get("workflow_id"),
                    "workflow_name": workflow_data.get("workflow_name", ""),
                    "parameters": workflow_data.get("parameters", {})
                }
            
            # Make HTTP request to MCP server
            response = await self.http_client.post(endpoint, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Workflow {workflow_id} executed successfully: {result.get('status')}")
            
            # Update last_run timestamp in database
            await self.update_last_run(workflow_id)
            
            # Log execution to database
            await self.log_execution(workflow_id, user_id, "success", result)
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error executing workflow {workflow_id}: {e}")
            await self.log_execution(workflow_id, user_id, "failed", {"error": str(e)})
        except Exception as e:
            logger.error(f"Error executing workflow {workflow_id}: {e}")
            await self.log_execution(workflow_id, user_id, "failed", {"error": str(e)})
    
    async def update_last_run(self, workflow_id: str):
        """Update the last_run timestamp for a scheduled workflow"""
        try:
            supabase.table("scheduled_workflows").update({
                "last_run": datetime.utcnow().isoformat()
            }).eq("id", workflow_id).execute()
        except Exception as e:
            logger.error(f"Error updating last_run for workflow {workflow_id}: {e}")
    
    async def log_execution(self, workflow_id: str, user_id: str, status: str, result: Dict[str, Any]):
        """Log workflow execution to database"""
        try:
            supabase.table("workflow_execution_logs").insert({
                "scheduled_workflow_id": workflow_id,
                "user_id": user_id,
                "status": status,
                "result": result,
                "executed_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            logger.error(f"Error logging execution for workflow {workflow_id}: {e}")
    
    async def check_and_execute_workflows(self):
        """Main job that checks and executes scheduled workflows"""
        logger.info("Checking for scheduled workflows to execute...")
        
        # Fetch all scheduled workflows
        workflows = await self.fetch_scheduled_workflows()
        logger.info(f"Found {len(workflows)} active scheduled workflows")
        
        # Check each workflow and execute if needed
        for workflow in workflows:
            try:
                if await self.should_execute_workflow(workflow):
                    logger.info(f"Workflow {workflow.get('id')} is due for execution")
                    await self.execute_workflow(workflow)
            except Exception as e:
                logger.error(f"Error processing workflow {workflow.get('id')}: {e}")
        
        logger.info("Finished checking scheduled workflows")
    
    def start(self):
        """Start the scheduler"""
        logger.info("Starting Blimp Cronjob Scheduler...")
        
        # Schedule the check job to run every N minutes
        self.scheduler.add_job(
            self.check_and_execute_workflows,
            CronTrigger(minute=f"*/{CHECK_INTERVAL_MINUTES}"),
            id="check_workflows",
            name="Check and execute scheduled workflows",
            replace_existing=True
        )
        
        # Also run at midnight every day (12:00 AM)
        self.scheduler.add_job(
            self.check_and_execute_workflows,
            CronTrigger(hour=0, minute=0),
            id="midnight_check",
            name="Midnight workflow check",
            replace_existing=True
        )
        
        self.scheduler.start()
        logger.info(f"Scheduler started. Checking workflows every {CHECK_INTERVAL_MINUTES} minutes and at midnight.")
    
    async def shutdown(self):
        """Shutdown the scheduler gracefully"""
        logger.info("Shutting down scheduler...")
        self.scheduler.shutdown()
        await self.http_client.aclose()
        logger.info("Scheduler stopped")


async def main():
    """Main entry point"""
    scheduler = WorkflowScheduler()
    
    try:
        scheduler.start()
        
        # Keep the script running
        while True:
            await asyncio.sleep(1)
            
    except (KeyboardInterrupt, SystemExit):
        await scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
