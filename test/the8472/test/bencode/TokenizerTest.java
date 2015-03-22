package the8472.test.bencode;

import static org.junit.Assert.*;


import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import the8472.bencode.Tokenizer;
import the8472.bencode.Tokenizer.TokenConsumer;

import static the8472.bencode.Utils.*;

public class TokenizerTest {
	
	Tokenizer t;
	
	@Before
	public void readFile() throws IOException, URISyntaxException {
		this.t = new Tokenizer();
		//this.file = ByteBuffer.wrap(Files.readAllBytes(Paths.get(this.getClass().getResource(("./ubuntu-14.10-desktop-amd64.iso.torrent")).toURI())));
	}
	
	@Test
	public void correctNumberHandling() {
		ByteBuffer num = str2buf("d3:fooi-17ee");
		
		CompletableFuture<Long> parsed = new CompletableFuture<>();
		
		t.tokenize(num, new TokenConsumer() {
			public void number(long result) {
				parsed.complete(result);								
			}
		});
		
		assertEquals(-17L, (long)parsed.getNow(0L));
	}

	@Test
	public void stopsBeforeTrailingContent() {
		ByteBuffer trailing = str2buf("de|trailing");
		
		CompletableFuture<Boolean> reachedEnd = new CompletableFuture<>();		
				
		this.t.tokenize(trailing, new TokenConsumer() {
			public void endOfRoot() {
				reachedEnd.complete(true);			
			}
		});
			
		assertEquals(2, trailing.position());
		assertTrue(reachedEnd.getNow(false));
	}

}
