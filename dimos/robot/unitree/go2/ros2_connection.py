# Copyright 2025-2026 Dimensional Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ROS 2 based connection for Unitree Go2, bypassing WebRTC entirely.

Use this when the Go2 robot publishes sensor data via ROS 2 topics
(e.g. via unitree_ros2_real or unitree_sdk2) and WebRTC is unavailable.

Usage::

    from dimos.robot.unitree.go2.ros2_connection import Go2ROS2Connection

    conn = Go2ROS2Connection(
        image_topic="/camera/color/image_raw",
        pointcloud_topic="/utlidar/cloud",
        odom_topic="/utlidar/robot_odom",
        cmd_vel_topic="/cmd_vel",
    )
    conn.start()
    conn.video_stream().subscribe(lambda img: print(img.shape))
"""

import logging
import time
import threading
from typing import Any

from reactivex.observable import Observable
from reactivex.subject import Subject

from dimos.msgs.geometry_msgs import PoseStamped, Twist
from dimos.msgs.sensor_msgs import Image, PointCloud2
from dimos.protocol.pubsub.impl.rospubsub import DimosROS, ROSTopic
from dimos.utils.decorators.decorators import simple_mcache
from dimos.utils.reactive import backpressure

logger = logging.getLogger(__name__)


class Go2ROS2Connection:
    """ROS 2 based connection for Go2 that satisfies Go2ConnectionProtocol.

    Subscribes to ROS 2 topics for camera, LiDAR, and odometry data,
    and publishes velocity commands via ROS 2.
    """

    def __init__(
        self,
        image_topic: str = "/camera/color/image_raw",
        pointcloud_topic: str = "/utlidar/cloud",
        odom_topic: str = "/utlidar/robot_odom",
        cmd_vel_topic: str = "/cmd_vel",
    ) -> None:
        self._image_topic = image_topic
        self._pointcloud_topic = pointcloud_topic
        self._odom_topic = odom_topic
        self._cmd_vel_topic = cmd_vel_topic

        self._ros: DimosROS | None = None
        self._image_subject: Subject[Image] = Subject()
        self._lidar_subject: Subject[PointCloud2] = Subject()
        self._odom_subject: Subject[PoseStamped] = Subject()
        self._unsubscribes: list[Any] = []

    def start(self) -> None:
        logger.info(
            "Starting Go2 ROS 2 connection: "
            f"image={self._image_topic}, "
            f"lidar={self._pointcloud_topic}, "
            f"odom={self._odom_topic}, "
            f"cmd_vel={self._cmd_vel_topic}"
        )

        self._ros = DimosROS(node_name="dimos_go2_ros2")
        self._ros.start()

        self._unsubscribes.append(
            self._ros.subscribe(
                ROSTopic(self._image_topic, Image),
                lambda msg, _: self._image_subject.on_next(msg),
            )
        )
        self._unsubscribes.append(
            self._ros.subscribe(
                ROSTopic(self._pointcloud_topic, PointCloud2),
                lambda msg, _: self._lidar_subject.on_next(msg),
            )
        )
        self._unsubscribes.append(
            self._ros.subscribe(
                ROSTopic(self._odom_topic, PoseStamped),
                lambda msg, _: self._odom_subject.on_next(msg),
            )
        )

        logger.info("Go2 ROS 2 connection started successfully")

    def stop(self) -> None:
        for unsub in self._unsubscribes:
            if callable(unsub):
                unsub()
        self._unsubscribes.clear()

        if self._ros:
            self._ros.stop()
            self._ros = None

        logger.info("Go2 ROS 2 connection stopped")

    @simple_mcache
    def video_stream(self) -> Observable[Image]:
        return backpressure(self._image_subject)

    @simple_mcache
    def lidar_stream(self) -> Observable[PointCloud2]:
        return backpressure(self._lidar_subject)

    @simple_mcache
    def odom_stream(self) -> Observable[PoseStamped]:
        return backpressure(self._odom_subject)

    def move(self, twist: Twist, duration: float = 0.0) -> bool:
        if self._ros is None:
            logger.warning("Cannot move: ROS 2 connection not started")
            return False

        topic = ROSTopic(self._cmd_vel_topic, Twist)

        if duration > 0:
            end_time = time.time() + duration
            while time.time() < end_time:
                self._ros.publish(topic, twist)
                time.sleep(0.01)
            # Send zero velocity to stop
            self._ros.publish(topic, Twist())
        else:
            self._ros.publish(topic, twist)

        return True

    def standup(self) -> bool:
        logger.info("standup: no-op in ROS 2 mode (use unitree_sdk2 service if needed)")
        return True

    def liedown(self) -> bool:
        logger.info("liedown: no-op in ROS 2 mode (use unitree_sdk2 service if needed)")
        return True

    def balance_stand(self) -> bool:
        logger.info("balance_stand: no-op in ROS 2 mode")
        return True

    def set_obstacle_avoidance(self, enabled: bool = True) -> None:
        logger.info(f"set_obstacle_avoidance({enabled}): no-op in ROS 2 mode")

    def publish_request(self, topic: str, data: dict[Any, Any]) -> dict[Any, Any]:
        logger.info(f"publish_request({topic}): no-op in ROS 2 mode")
        return {"status": "ok", "message": "ROS 2 mode - not supported"}


__all__ = ["Go2ROS2Connection"]
