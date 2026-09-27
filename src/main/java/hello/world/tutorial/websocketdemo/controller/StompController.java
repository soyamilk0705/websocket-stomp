package hello.world.tutorial.websocketdemo.controller;

import hello.world.tutorial.websocketdemo.dto.ReqDto;
import hello.world.tutorial.websocketdemo.dto.ResDto;
import hello.world.tutorial.websocketdemo.dto.ResSessionDto;
import hello.world.tutorial.websocketdemo.listener.StompEventListener;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Controller
@Slf4j
public class StompController {

    private final StompEventListener eventListener;
    private final SimpMessagingTemplate template;
    private final TaskScheduler taskScheduler;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> sessionMap = new ConcurrentHashMap<>();

    public StompController(StompEventListener eventListener, SimpMessagingTemplate template, TaskScheduler taskScheduler) {
        this.eventListener = eventListener;
        this.template = template;
        this.taskScheduler = taskScheduler;
    }

//    @MessageMapping("/hello") // /app/hello
//    @SendTo("/topic/hello")
//    public ResDto basic(ReqDto reqDto){
//        log.info("reqDto: {}", reqDto);
//        return new ResDto(reqDto.getMessage().toUpperCase(), LocalDateTime.now());
//    }//


    @MessageMapping("/hello") // /app/hello
    @SendTo({"/topic/hello", "/topic/hello2"})
    public ResDto basic2(ReqDto reqDto, Message<ReqDto> message, MessageHeaders headers){
        log.info("reqDto: {}", reqDto);
        log.info("message: {}", message);
        log.info("headers: {}", headers);
        return new ResDto(reqDto.getMessage().toUpperCase(), LocalDateTime.now());
    }


    @MessageMapping("/hello/{detail}") // /app/hello
    @SendTo({"/topic/hello", "/topic/hello2"})
    public ResDto detail(@DestinationVariable("detail") String detail, ReqDto reqDto){
        log.info("reqDto: {}", reqDto);
        log.info("detail: {}", detail);
        return new ResDto(reqDto.getMessage().toUpperCase(), LocalDateTime.now());
    }


    @MessageMapping("/sessions") // /app/hello
    @SendToUser("/queue/sessions")
    public ResSessionDto sessions(ReqDto reqDto, MessageHeaders headers){
        log.info("reqDto: {}", reqDto);
        String sessionId = headers.get("simpSessionId").toString();
        log.info("sessionId: {}", sessionId);

        Set<String> sessions = eventListener.getSessions();

        return new ResSessionDto(sessions.size(), sessions.stream().toList(), sessionId, LocalDateTime.now());
    }


    @MessageMapping("/code1") // /app/hello
//    @SendTo({"/topic/hello", "/topic/hello2"})
    public void code1(ReqDto reqDto, Message<ReqDto> message, MessageHeaders headers){
        log.info("reqDto: {}", reqDto);
        log.info("message: {}", message);
        log.info("headers: {}", headers);

        ResDto resDto = new ResDto(reqDto.getMessage().toUpperCase(), LocalDateTime.now());
        template.convertAndSend("/topic/hello", resDto);
    }



    @MessageMapping("/code2") // /app/hello
//    @SendToUser("/queue/sessions")
    public void code2(ReqDto reqDto, MessageHeaders headers){
        log.info("reqDto: {}", reqDto);
        String sessionId = headers.get("simpSessionId").toString();
        log.info("sessionId: {}", sessionId);

        Set<String> sessions = eventListener.getSessions();

        ResSessionDto resSessionDto = new ResSessionDto(sessions.size(), sessions.stream().toList(), sessionId, LocalDateTime.now());
        template.convertAndSendToUser(sessionId, "/queue/sessions", resSessionDto, createHeaders(sessionId));
    }

    MessageHeaders createHeaders(@Nullable String sessionId){
        SimpMessageHeaderAccessor headerAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headerAccessor.setSessionId(sessionId);
        headerAccessor.setLeaveMutable(true);
        return headerAccessor.getMessageHeaders();
    }

    @MessageMapping("/start")
    public void start(ReqDto reqDto, MessageHeaders headers){
        log.info("headers: {}", headers);
        String sessionId = headers.get("simpSessionId").toString();
        log.info("sessionId: {}", sessionId);

        ScheduledFuture<?> scheduledFuture = taskScheduler.scheduleAtFixedRate(() -> {
            Random random = new Random();
            int currentPrice = random.nextInt(100);
            template.convertAndSendToUser(sessionId, "/queue/trade", currentPrice, createHeaders(sessionId));
        }, Duration.ofSeconds(3));
        sessionMap.put(sessionId, scheduledFuture);
    }


    @MessageMapping("/stop")
    public void stop(ReqDto reqDto, MessageHeaders headers){
        log.info("headers: {}", headers);
        String sessionId = headers.get("simpSessionId").toString();
        log.info("sessionId: {}", sessionId);

        ScheduledFuture<?> remove = sessionMap.remove(sessionId);
        remove.cancel(true);
    }

    @MessageMapping("/exception")
    @SendTo("/topic/hello")
    public void exception(ReqDto request, MessageHeaders headers) throws Exception {
        log.info("request: {}", request);
        String message = request.getMessage();
        switch (message){
            case "runtime":
                throw new RuntimeException();
            case "nullPointer":
                throw new NullPointerException();
            case "io":
                throw new IOException();
            case "exception":
                throw new Exception();
            default:
                throw new InvalidParameterException();
        }
    }
}
