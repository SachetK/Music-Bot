package com.github.SachetK.musicBot;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayer;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.player.DefaultAudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.source.AudioSourceManagers;
import com.sedmelluq.discord.lavaplayer.track.playback.NonAllocatingAudioFrameBuffer;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.VoiceState;
import discord4j.core.object.entity.Member;
import discord4j.voice.AudioProvider;
import io.github.cdimascio.dotenv.Dotenv;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MusicBot {
    private static final Map<String, Command> commands = new HashMap<>();
    private static final AudioPlayerManager playerManager;

    static {
        playerManager = new DefaultAudioPlayerManager();

        playerManager.getConfiguration()
                .setFrameBufferFactory(NonAllocatingAudioFrameBuffer::new);

        AudioSourceManagers.registerRemoteSources(playerManager);
        AudioSourceManagers.registerLocalSource(playerManager);

        final AudioPlayer player = playerManager.createPlayer();

        AudioProvider provider = new LavaPlayerAudioProvider(player);

        final TrackScheduler scheduler = new TrackScheduler(player);

        commands.put("play", event -> Mono.justOrEmpty(event.getMessage().getContent())
                .map(content -> Arrays.asList(content.split(" ")))
                .doOnNext(command -> playerManager.loadItem(command.get(1), scheduler))
                .then());

        commands.put("join", event -> Mono.justOrEmpty(event.getMember())
                .flatMap(Member::getVoiceState)
                .flatMap(VoiceState::getChannel)
                .flatMap(channel -> channel.join(spec -> spec.setProvider(provider)))
                .then());

        commands.put("ping", event -> event.getMessage().getChannel()
                .flatMap(channel -> channel.createMessage("Pong!"))
                .then());

        commands.put("message", event -> event.getMessage().getChannel()
                .flatMap(channel -> channel.createMessage("I love you!"))
                .then());
    }

    public static void main(final String[] args) {
        Dotenv dotenv = Dotenv.configure().load();
        final GatewayDiscordClient client = DiscordClientBuilder.create(dotenv.get("DISCORD_BOT_ID")).build()
                .login()
                .block();
        assert client != null;
        client.getEventDispatcher().on(MessageCreateEvent.class)
            .flatMap(event -> Mono.just(event.getMessage().getContent())
                .flatMap(content -> Flux.fromIterable(commands.entrySet())
                    .filter(entry -> content.startsWith('!' + entry.getKey()))
                    .flatMap(entry -> entry.getValue().execute(event))
                    .next()))
            .subscribe();
        client.onDisconnect().block();
    }
}
