package com.zy.flinktime

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * create 2020-11-21
 * author zy
 * desc utc的触发器
 */
class UtcTrigger extends Trigger[MyTime,TimeWindow]{
  //当每个元素被添加窗口时调用
  override def onElement(t: MyTime, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onElement")
    TriggerResult.FIRE_AND_PURGE
  }

  //当注册的处理时间计时器被触发时调用
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onProcessingTime")
    TriggerResult.CONTINUE
  }

  //当注册的事件时间计时器被触发时调用
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    println("触发器onEventTime")
    TriggerResult.FIRE_AND_PURGE
  }

  //在清除（removal）窗口时调用
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    println("触发器clear")
  }
}
