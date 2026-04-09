"""CDK app entry point."""

import aws_cdk as cdk

from stack import ComplianceCopilotStack

app = cdk.App()
ComplianceCopilotStack(
    app,
    "ComplianceCopilotStack",
    env=cdk.Environment(region="us-east-1"),
)
app.synth()
