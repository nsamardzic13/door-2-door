import aws_cdk as core
import aws_cdk.assertions as assertions

from door2_door.door2_door_stack import Door2DoorStack

# example tests. To run these tests, uncomment this file along with the example
# resource in door2_door/door2_door_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = Door2DoorStack(app, "door2-door")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
